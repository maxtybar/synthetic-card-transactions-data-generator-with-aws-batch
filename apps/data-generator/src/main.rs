use std::env;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_config::Region;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use chrono::Datelike;
use rayon::prelude::*;
use rand::{SeedableRng, seq::SliceRandom, rngs::StdRng};
use log::{info, warn, error, debug};

mod common;
mod dynamodb_client;
mod data_generator;

use data_generator::*;
use dynamodb_client::get_100_random_hash_pans;

#[derive(Debug)]
struct ThreadResult {
    authorization_batch: RecordBatch,
    authorization_hash_batch: RecordBatch,
    clearing_batch: RecordBatch,
    clearing_hash_batch: RecordBatch,
    chargeback_batch: Option<RecordBatch>,
    chargeback_hash_batch: Option<RecordBatch>,
}

async fn upload_to_respective_buckets(
    s3_client: &S3Client,
    main_bucket: &str,
    specialized_bucket: &str,
    table_name: &str,
    specialized_table_name: &str,
    data: Vec<u8>,
    job_index: i32,
    thread_id: i32,
    year: i32,
    month: &str,
    day: &str,
) -> Result<()> {
    let main_key = format!("{}/{}/{}/{}/job_{}_thread_{}.parquet", table_name, year, month, day, job_index, thread_id);
    let specialized_key = format!("{}/{}/{}/{}/job_{}_thread_{}.parquet", specialized_table_name, year, month, day, job_index, thread_id);
    
    tokio::try_join!(
        upload_with_retry(s3_client, main_bucket, &main_key, &data, 3),
        upload_with_retry(s3_client, specialized_bucket, &specialized_key, &data, 3)
    )?;
    
    Ok(())
}

async fn upload_with_retry(s3_client: &S3Client, bucket: &str, key: &str, body: &[u8], max_retries: u32) -> Result<()> {
    for attempt in 0..max_retries {
        let body_stream = ByteStream::from(body.to_vec());
        match s3_client.put_object().bucket(bucket).key(key).body(body_stream).send().await {
            Ok(_) => {
                debug!("Upload successful: {} (attempt {})", key, attempt + 1);
                return Ok(());
            },
            Err(e) => {
                let error_details = if let Some(service_err) = e.as_service_error() {
                    let error_code = service_err.code().unwrap_or("Unknown");
                    let error_message = service_err.message().unwrap_or("No message");
                    format!("Service error - Code: {}, Message: {}", error_code, error_message)
                } else {
                    format!("SDK error: {}", e)
                };
                
                if attempt == max_retries - 1 {
                    error!("Upload failed after {} attempts: {} - {}", max_retries, key, error_details);
                    return Err(anyhow::anyhow!("Failed to upload {} after {} attempts. Last error: {}", key, max_retries, error_details));
                }
                
                let delay = std::time::Duration::from_millis(1000 * (2_u64.pow(attempt)));
                warn!("Upload attempt {} failed for {}: {}. Retrying in {:?}...", attempt + 1, key, error_details, delay);
                tokio::time::sleep(delay).await;
            }
        }
    }
    unreachable!()
}

fn calculate_partition_date(job_index: i32) -> (i32, String, String) {
    let initial_load = std::env::var("INITIAL_LOAD").unwrap_or_else(|_| "true".to_string());
    
    if initial_load.to_lowercase() == "true" {
        calculate_initial_partition_date(job_index)
    } else {
        calculate_nightly_partition_date()
    }
}

fn calculate_initial_partition_date(job_index: i32) -> (i32, String, String) {
    let start_date = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
    let end_date = chrono::Utc::now().naive_utc().date() - chrono::Duration::days(7);
    let total_days = (end_date - start_date).num_days() as i32 + 1; // +1 to include end_date
    
    let mut hasher = DefaultHasher::new();
    job_index.hash(&mut hasher);
    let hash_value = hasher.finish();
    
    let day_offset = (hash_value % total_days as u64) as i32;
    let target_date = start_date + chrono::Duration::days(day_offset as i64);
    
    (target_date.year(), format!("{:02}", target_date.month()), format!("{:02}", target_date.day()))
}

fn calculate_nightly_partition_date() -> (i32, String, String) {
    let initial_end_date = chrono::Utc::now().naive_utc().date() - chrono::Duration::days(7);
    let days_since_initial_end = (chrono::Utc::now().naive_utc().date() - initial_end_date).num_days();
    
    // Generate dates from (initial_end_date + 1) to today
    let nightly_job_index = std::env::var("AWS_BATCH_JOB_ARRAY_INDEX")
        .unwrap_or_else(|_| "0".to_string())
        .parse::<i32>()
        .unwrap_or(0);
    
    let day_offset = nightly_job_index % days_since_initial_end as i32;
    let target_date = initial_end_date + chrono::Duration::days(day_offset as i64 + 1);
    
    (target_date.year(), format!("{:02}", target_date.month()), format!("{:02}", target_date.day()))
}

async fn upload_thread_results(
    s3_client: &S3Client,
    buckets: &(String, String, String, String),
    thread_result: ThreadResult,
    job_index: i32,
    thread_id: i32,
    year: i32,
    month: &str,
    day: &str,
) -> Result<()> {
    let (payment_data_bucket, auth_bucket, clearing_bucket, chargeback_bucket) = buckets;
    
    let mut upload_tasks = vec![];
    
    upload_tasks.push(tokio::spawn({
        let s3_client = s3_client.clone();
        let payment_data_bucket = payment_data_bucket.clone();
        let auth_bucket = auth_bucket.clone();
        let month = month.to_string();
        let day = day.to_string();
        async move {
            let data = generate_parquet_data(thread_result.authorization_batch).await?;
            upload_to_respective_buckets(&s3_client, &payment_data_bucket, &auth_bucket, "authorization", "authorization", data, job_index, thread_id, year, &month, &day).await
        }
    }));
    
    upload_tasks.push(tokio::spawn({
        let s3_client = s3_client.clone();
        let payment_data_bucket = payment_data_bucket.clone();
        let auth_bucket = auth_bucket.clone();
        let month = month.to_string();
        let day = day.to_string();
        async move {
            let data = generate_parquet_data(thread_result.authorization_hash_batch).await?;
            upload_to_respective_buckets(&s3_client, &payment_data_bucket, &auth_bucket, "authorization_hash", "authorization_hash", data, job_index, thread_id, year, &month, &day).await
        }
    }));
    
    upload_tasks.push(tokio::spawn({
        let s3_client = s3_client.clone();
        let payment_data_bucket = payment_data_bucket.clone();
        let clearing_bucket = clearing_bucket.clone();
        let month = month.to_string();
        let day = day.to_string();
        async move {
            let data = generate_parquet_data(thread_result.clearing_batch).await?;
            upload_to_respective_buckets(&s3_client, &payment_data_bucket, &clearing_bucket, "clearing", "clearing", data, job_index, thread_id, year, &month, &day).await
        }
    }));
    
    upload_tasks.push(tokio::spawn({
        let s3_client = s3_client.clone();
        let payment_data_bucket = payment_data_bucket.clone();
        let clearing_bucket = clearing_bucket.clone();
        let month = month.to_string();
        let day = day.to_string();
        async move {
            let data = generate_parquet_data(thread_result.clearing_hash_batch).await?;
            upload_to_respective_buckets(&s3_client, &payment_data_bucket, &clearing_bucket, "clearing_hash", "clearing_hash", data, job_index, thread_id, year, &month, &day).await
        }
    }));
    
    if let (Some(chargeback_batch), Some(chargeback_hash_batch)) = (thread_result.chargeback_batch, thread_result.chargeback_hash_batch) {
        upload_tasks.push(tokio::spawn({
            let s3_client = s3_client.clone();
            let payment_data_bucket = payment_data_bucket.clone();
            let chargeback_bucket = chargeback_bucket.clone();
            let month = month.to_string();
            let day = day.to_string();
            async move {
                let data = generate_parquet_data(chargeback_batch).await?;
                upload_to_respective_buckets(&s3_client, &payment_data_bucket, &chargeback_bucket, "chargeback", "chargeback", data, job_index, thread_id, year, &month, &day).await
            }
        }));
        
        upload_tasks.push(tokio::spawn({
            let s3_client = s3_client.clone();
            let payment_data_bucket = payment_data_bucket.clone();
            let chargeback_bucket = chargeback_bucket.clone();
            let month = month.to_string();
            let day = day.to_string();
            async move {
                let data = generate_parquet_data(chargeback_hash_batch).await?;
                upload_to_respective_buckets(&s3_client, &payment_data_bucket, &chargeback_bucket, "chargeback_hash", "chargeback_hash", data, job_index, thread_id, year, &month, &day).await
            }
        }));
    }
    
    for task in upload_tasks {
        task.await??;
    }
    
    Ok(())
}

async fn generate_parquet_data(
    batch: arrow::record_batch::RecordBatch,
) -> Result<Vec<u8>> {
    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    
    Ok(buffer)
}

async fn generate_thread_data(
    thread_id: i32,
    job_index: i32,
    partition_job_order: i64,
    num_threads: i32,
    process_date: String,
    dynamodb_client: DynamoDbClient,
    hash_pan_table_name: String,
    num_rows: usize,
    chargeback_percentage: f64,
) -> Result<ThreadResult> {
    info!("🔄 Thread {} starting complete table generation for {} rows", thread_id, num_rows);
    
    // Calculate unique thread seed to prevent collisions across jobs and threads
    let thread_seed = (job_index as u64) * 1000 + (thread_id as u64);
    
    // Get 100 hash_pans for this thread to randomly select from during generation
    debug!("Thread {} retrieving 100 hash_pans from DynamoDB", thread_id);
    let hash_pan_pool = get_100_random_hash_pans(&dynamodb_client, &hash_pan_table_name, thread_id).await?;
    info!("🔑 Thread {} retrieved {} hash_pans for random selection", thread_id, hash_pan_pool.len());
    
    // Generate unique row seeds for this thread (job_index * 1000 * 100000 + thread_id * 100000 + row_idx)
    let row_seeds: Vec<u64> = (0..num_rows).map(|row_idx| {
        thread_seed * 100000 + (row_idx as u64)
    }).collect();
    
    // Generate chargeback row seeds by selecting from authorization transactions
    let total_chargebacks_needed = (num_rows as f64 * chargeback_percentage) as usize;
    let chargeback_row_seeds = if total_chargebacks_needed > 0 {
        let chargeback_seed = (job_index as u64).wrapping_mul(1000).wrapping_add(thread_id as u64);
        let mut rng = StdRng::seed_from_u64(chargeback_seed);
        
        row_seeds.choose_multiple(&mut rng, total_chargebacks_needed)
            .cloned()
            .collect::<Vec<u64>>()
    } else {
        Vec::new()
    };
    
    // Generate all 6 tables in parallel using Rayon
    info!("✅ Thread {} starting parallel generation of all 6 tables", thread_id);
    
    let ((authorization_batch, authorization_hash_batch), ((clearing_batch, clearing_hash_batch), (chargeback_batch, chargeback_hash_batch))) = rayon::join(
        || {
            let auth_batch = generate_authorization_batch(num_rows, &row_seeds, partition_job_order, thread_id, num_threads, &chargeback_row_seeds, &hash_pan_pool, &process_date);
            info!("✅ Thread {} completed authorization table ({} rows)", thread_id, auth_batch.num_rows());
            let auth_hash_batch = generate_authorization_hash_batch(num_rows, &row_seeds, partition_job_order, thread_id, num_threads, &chargeback_row_seeds, &hash_pan_pool, &process_date);
            info!("✅ Thread {} completed authorization_hash table ({} rows)", thread_id, auth_hash_batch.num_rows());
            (auth_batch, auth_hash_batch)
        },
        || {
            rayon::join(
                || {
                    let clearing_batch = generate_clearing_batch(num_rows, &row_seeds, partition_job_order, thread_id, num_threads, &chargeback_row_seeds, &hash_pan_pool, &process_date);
                    info!("✅ Thread {} completed clearing table ({} rows)", thread_id, clearing_batch.num_rows());
                    let clearing_hash_batch = generate_clearing_hash_batch(num_rows, &row_seeds, partition_job_order, thread_id, num_threads, &chargeback_row_seeds, &hash_pan_pool, &process_date);
                    info!("✅ Thread {} completed clearing_hash table ({} rows)", thread_id, clearing_hash_batch.num_rows());
                    (clearing_batch, clearing_hash_batch)
                },
                || {
                    if !chargeback_row_seeds.is_empty() {
                        info!("✅ Thread {} generating chargeback tables for {} transactions", thread_id, chargeback_row_seeds.len());
                        let (cb_batch, cb_hash_batch) = rayon::join(
                            || {
                                let batch = generate_chargeback_batch(chargeback_row_seeds.len(), &chargeback_row_seeds, partition_job_order, thread_id, num_threads, &hash_pan_pool, &process_date);
                                info!("✅ Thread {} completed chargeback table ({} rows)", thread_id, batch.num_rows());
                                batch
                            },
                            || {
                                let batch = generate_chargeback_hash_batch(chargeback_row_seeds.len(), &chargeback_row_seeds, partition_job_order, thread_id, num_threads, &hash_pan_pool, &process_date);
                                info!("✅ Thread {} completed chargeback_hash table ({} rows)", thread_id, batch.num_rows());
                                batch
                            }
                        );
                        (Some(cb_batch), Some(cb_hash_batch))
                    } else {
                        info!("✅ Thread {} skipping chargeback tables (no chargeback transactions)", thread_id);
                        (None, None)
                    }
                }
            )
        }
    );
    
    info!("✅ Thread {} completed all table generation", thread_id);
    
    Ok(ThreadResult {
        authorization_batch,
        authorization_hash_batch,
        clearing_batch,
        clearing_hash_batch,
        chargeback_batch,
        chargeback_hash_batch,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::init();
    
    let array_index: i32 = env::var("AWS_BATCH_JOB_ARRAY_INDEX")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("Invalid array index");
    
    let job_index_offset: i32 = env::var("JOB_INDEX_OFFSET")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .expect("Invalid job index offset");
    
    let job_index = array_index + job_index_offset;
    
    info!("🔍 Job index calculation: AWS_BATCH_JOB_ARRAY_INDEX={}, JOB_INDEX_OFFSET={}, final_job_index={}", array_index, job_index_offset, job_index);
    info!("🚀 Job {} starting with array_index={}, offset={}", job_index, array_index, job_index_offset);
    
    let payment_data_bucket_name = env::var("PAYMENT_DATA_BUCKET_NAME").expect("PAYMENT_DATA_BUCKET_NAME not set");
    let clearing_bucket_name = env::var("CLEARING_BUCKET_NAME").expect("CLEARING_BUCKET_NAME not set");
    let authorization_bucket_name = env::var("AUTHORIZATION_BUCKET_NAME").expect("AUTHORIZATION_BUCKET_NAME not set");
    let chargeback_bucket_name = env::var("CHARGEBACK_BUCKET_NAME").expect("CHARGEBACK_BUCKET_NAME not set");
    let hash_pan_table_name = env::var("HASH_PAN_TABLE_NAME").expect("HASH_PAN_TABLE_NAME not set");
    let partition_counter_table_name = env::var("PARTITION_COUNTER_TABLE_NAME").expect("PARTITION_COUNTER_TABLE_NAME not set");
    
    let num_rows = env::var("NUM_OF_ROWS")
        .unwrap_or_else(|_| "250000".to_string())
        .parse::<usize>()
        .unwrap_or(250000);
    
    let chargeback_percentage = env::var("CHARGEBACK_PERCENTAGE")
        .unwrap_or_else(|_| "0.1".to_string())
        .parse::<f64>()
        .unwrap_or(0.1) / 100.0; // Convert percentage to decimal (0.1% -> 0.001)
    
    let aws_region = env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let dynamodb_region = env::var("DYNAMODB_REGION").unwrap_or_else(|_| aws_region.clone());
    
    info!("⚙️ Job {} configuration: rows={}, chargeback_pct={:.1}%, region={}", 
          job_index, num_rows, chargeback_percentage * 100.0, aws_region);
    
    let start_time = std::time::Instant::now();
    
    let (year, month, day) = calculate_partition_date(job_index);
    let process_date = format!("{}-{:02}-{:02}", year, month.parse::<u32>().unwrap_or(1), day.parse::<u32>().unwrap_or(1));
    info!("📅 Job {} partition date: {}-{}-{} (process_date: {})", job_index, year, month, day, process_date);
    
    let gen_start = std::time::Instant::now();
    
    info!("🔧 Configuring AWS clients with extended timeouts...");
    
    // AWS clients with extended timeouts for container startup  
    let timeout_config = aws_config::timeout::TimeoutConfig::builder()
        .operation_timeout(std::time::Duration::from_secs(60))
        .operation_attempt_timeout(std::time::Duration::from_secs(30))
        .build();
    
    // Add retry configuration for credential resolution
    let retry_config = aws_config::retry::RetryConfig::adaptive()
        .with_max_attempts(3);
    
    debug!("Loading S3 configuration...");    
    let s3_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(aws_region.clone()))
        .timeout_config(timeout_config.clone())
        .retry_config(retry_config.clone())
        .load()
        .await;
    info!("✅ S3 client configured for region {}", aws_region);
    
    let s3_client = S3Client::new(&s3_config);
    
    debug!("Loading DynamoDB configuration...");
    let dynamodb_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(dynamodb_region.clone()))
        .timeout_config(timeout_config)
        .retry_config(retry_config)
        .load()
        .await;
    info!("✅ DynamoDB client configured for region {}", dynamodb_region);
    
    let dynamodb_client = DynamoDbClient::new(&dynamodb_config);
    
    // Create unique job identifier (replace colon with underscore for DynamoDB)
    let job_id = format!("{}_{}", 
        env::var("AWS_BATCH_JOB_ID").unwrap_or_else(|_| "local".to_string()).replace(":", "_"),
        array_index
    );
    info!("🆔 Job unique identifier: {}", job_id);
    
    // Get atomic partition job order from DynamoDB
    info!("🔢 Getting partition job order for {} from DynamoDB...", process_date);
    info!("🔍 Using partition counter table: {}", partition_counter_table_name);
    let partition_job_order = dynamodb_client::get_partition_job_order(&process_date, &dynamodb_client, &partition_counter_table_name, &job_id)
        .await?;
    info!("✅ Job {} assigned partition order {} for date {}", job_index, partition_job_order, process_date);
    
    info!("🚀 Job {} starting parallel thread generation", job_index);
    let available_threads = rayon::current_num_threads();
    let num_threads = 3;
    info!("🧵 Job {} using {} threads (forced, available: {}), {} rows per thread", job_index, num_threads, available_threads, num_rows);
    
    let buckets = (payment_data_bucket_name, authorization_bucket_name, clearing_bucket_name, chargeback_bucket_name);
    
    (1..=num_threads)
        .into_par_iter()
        .map(|thread_id| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let thread_result = generate_thread_data(
                    thread_id as i32, job_index, partition_job_order, num_threads, process_date.clone(),
                    dynamodb_client.clone(), hash_pan_table_name.clone(), 
                    num_rows, chargeback_percentage
                ).await?;
                
                upload_thread_results(
                    &s3_client, &buckets, thread_result, 
                    job_index, thread_id as i32, year, &month, &day
                ).await
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    
    // Mark job as completed and remove from active_jobs
    info!("🏁 Marking job {} as completed", job_id);
    dynamodb_client::mark_job_completed(&process_date, &dynamodb_client, &partition_counter_table_name, &job_id).await?;
    
    info!("✅ Job {} completed all {} threads in {:.1}s", job_index, num_threads, gen_start.elapsed().as_secs_f64());
    info!("🎉 Job {} finished successfully in {:.1}s", job_index, start_time.elapsed().as_secs_f64());
    Ok(())
}
