use aws_sdk_batch::Client as BatchClient;
use aws_sdk_batch::types::{ContainerOverrides, KeyValuePair};
use clap::Parser;
use futures::stream::{self, StreamExt};
use std::time::Instant;
use anyhow::Result;

const MAX_ARRAY_SIZE: i32 = 1000;
const NUM_OF_ROWS: i32 = 1000000;
const CONCURRENT_SUBMISSIONS: usize = 50;
const BASE_TB: i32 = 655; 

// Base configuration
fn get_base_jobs() -> i32 {
    let threads_per_job: i32 = 3; // 3 threads per job
    1_107_000 / threads_per_job
}

fn calculate_job_parameters(target_tb: i32) -> (i32, i32) {
    // Calculate total jobs needed based on target TB
    let base_jobs = get_base_jobs();
    let total_jobs = (base_jobs as f64 * target_tb as f64 / BASE_TB as f64) as i32;
    
    // Always use max array size 
    // (will be split into multiple arrays of size MAX_ARRAY_SIZE each)
    let array_size = MAX_ARRAY_SIZE;
    
    (total_jobs, array_size)
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    spot_queue_name: String,
    #[arg(long)]
    ondemand_queue_name: String,
    #[arg(short, long)]
    job_definition: String,
    #[arg(short, long)]
    payment_data_bucket_name: String,
    #[arg(long)]
    clearing_bucket_name: String,
    #[arg(long)]
    authorization_bucket_name: String,
    #[arg(long)]
    chargeback_bucket_name: String,
    #[arg(long)]
    hash_pan_table_name: String,
    #[arg(long)]
    partition_counter_table_name: String,
    #[arg(long)]
    card_brand: String,
    #[arg(long)]
    network_brand: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let start_time = Instant::now();
    
    // Get target TB from environment variable
    let target_tb: i32 = std::env::var("TARGET_TB")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .unwrap_or(500);
    
    let initial_load: bool = std::env::var("INITIAL_LOAD")
        .unwrap_or_else(|_| "true".to_string())
        .to_lowercase() == "true";
    
    // Calculate job parameters dynamically
    let (total_jobs, array_size) = calculate_job_parameters(target_tb);
    
    let queues = vec![args.spot_queue_name.clone(), args.ondemand_queue_name.clone()];

    let today = chrono::Utc::now().naive_utc().date();
    let today_str = today.format("%Y-%m-%d").to_string();
    let seven_days_ago = today - chrono::Duration::days(7);
    let seven_days_ago_str = seven_days_ago.format("%Y-%m-%d").to_string();
    let six_days_ago = today - chrono::Duration::days(6);
    let six_days_ago_str = six_days_ago.format("%Y-%m-%d").to_string();
    
    let cpu_per_job = 4; // 4 vCPUs per job
    let memory_per_job_gb = 28; // 28GB RAM per job
    let threads_per_job = 3; // 3 threads per job
    
    println!("--- Dynamic 6-Table Combined Upload Configuration ---");
    println!("Target data size: {}TB", target_tb);
    println!("Card brand: {}", args.card_brand);
    println!("Network brand: {}", args.network_brand);
    println!("Load type: {}", if initial_load { 
        format!("Initial Load (2020-01-01 to {} inclusive - UTC time)", seven_days_ago_str) 
    } else { 
        format!("Nightly Load ({} to {} inclusive - covers last 7 days - UTC time)", six_days_ago_str, today_str) 
    });
    println!("Total jobs: {} (each generates 750k rows total: 3 threads × 250k rows × 6 tables)", total_jobs);
    println!("Array size: {}", array_size);
    println!("Container specs: {} vCPUs + {}GB RAM per job (optimized for combined uploads)", cpu_per_job, memory_per_job_gb);
    println!("Multi-threaded generation: {} threads per job → combined into 1 upload per table", threads_per_job);
    println!("Dual bucket uploads: Combined bucket + 3 specialized buckets (clearing, authorization, chargeback)");
    println!("Using 2 queues for maximum scaling: {} jobs each", total_jobs / 2);
    println!("------------------------------\n");

    let mut requests = Vec::new();
    
    // Calculate how many array jobs we need to submit all jobs
    let total_arrays_needed = (total_jobs + array_size - 1) / array_size; // Ceiling division
    let jobs_per_queue = total_jobs / 2;
    let arrays_per_queue = (total_arrays_needed + 1) / 2; // Split arrays between queues
    
    println!("Using 2 queues with {} arrays each (max {} jobs per array)", arrays_per_queue, array_size);
    
    // Create multiple array jobs for each queue
    let mut job_offset = 0;
    for queue_idx in 0..2 {
        let queue_name = &queues[queue_idx];
        let remaining_jobs = total_jobs - job_offset;
        let queue_jobs = std::cmp::min(jobs_per_queue, remaining_jobs);
        
        // Submit multiple arrays for this queue
        let mut queue_job_offset = job_offset;
        while queue_job_offset < job_offset + queue_jobs {
            let remaining_in_queue = (job_offset + queue_jobs) - queue_job_offset;
            let current_array_size = std::cmp::min(array_size, remaining_in_queue);
            
            requests.push((current_array_size, queue_name.clone(), queue_job_offset));
            queue_job_offset += current_array_size;
        }
        
        job_offset += queue_jobs;
    }

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = BatchClient::new(&config);

    let requests_len = requests.len();
    println!("🔧 Using job definition: {}", args.job_definition);
    println!("🔧 Submitting {} batches to queues", requests_len);
    
    let mut submission_stream = stream::iter(requests.into_iter().enumerate())
        .map(|(batch_num, (array_size, queue_name, start_index))| {
            println!("🚀 Submitting batch {} to queue: {}", batch_num + 1, queue_name);
            let client = client.clone();
            let job_definition = args.job_definition.clone();
            let payment_data_bucket_name = args.payment_data_bucket_name.clone();
            let clearing_bucket_name = args.clearing_bucket_name.clone();
            let authorization_bucket_name = args.authorization_bucket_name.clone();
            let chargeback_bucket_name = args.chargeback_bucket_name.clone();
            let hash_pan_table_name = args.hash_pan_table_name.clone();
            let partition_counter_table_name = args.partition_counter_table_name.clone();
            let card_brand = args.card_brand.clone();
            let network_brand = args.network_brand.clone();
            
            tokio::spawn(async move {
                let job_name = format!("6table-multiple-buckets-batch-{}", batch_num + 1);
                let start_index = start_index;
                
                let container_overrides = ContainerOverrides::builder()
                    .environment(KeyValuePair::builder()
                        .name("PAYMENT_DATA_BUCKET_NAME")
                        .value(&payment_data_bucket_name)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("CLEARING_BUCKET_NAME")
                        .value(&clearing_bucket_name)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("AUTHORIZATION_BUCKET_NAME")
                        .value(&authorization_bucket_name)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("CHARGEBACK_BUCKET_NAME")
                        .value(&chargeback_bucket_name)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("HASH_PAN_TABLE_NAME")
                        .value(&hash_pan_table_name)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("PARTITION_COUNTER_TABLE_NAME")
                        .value(&partition_counter_table_name)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("JOB_INDEX_OFFSET")
                        .value(start_index.to_string())
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("NUM_OF_ROWS")
                        .value(NUM_OF_ROWS.to_string())
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("CHARGEBACK_PERCENTAGE")
                        .value(std::env::var("CHARGEBACK_PERCENTAGE").unwrap_or_else(|_| "0.1".to_string()))
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("INITIAL_LOAD")
                        .value(initial_load.to_string())
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("AWS_DEFAULT_REGION")
                        .value(std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()))
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("CARD_BRAND")
                        .value(card_brand)
                        .build())
                    .environment(KeyValuePair::builder()
                        .name("NETWORK_BRAND")
                        .value(network_brand)
                        .build())
                    .build();

                let result = client
                    .submit_job()
                    .job_name(&job_name)
                    .job_queue(&queue_name)
                    .job_definition(&job_definition)
                    .array_properties(
                        aws_sdk_batch::types::ArrayProperties::builder()
                            .size(array_size)
                            .build()
                    )
                    .container_overrides(container_overrides)
                    .send()
                    .await;

                match result {
                    Ok(response) => {
                        println!("✅ Batch {} submitted: {} (jobs {}-{}) to {}", 
                                batch_num + 1, 
                                response.job_id().unwrap_or("unknown"),
                                start_index,
                                start_index + array_size - 1,
                                queue_name);
                        Ok::<i32, anyhow::Error>(array_size)
                    }
                    Err(e) => {
                        eprintln!("❌ Batch {} failed: {:?}", batch_num + 1, e);
                        Err(e.into())
                    }
                }
            })
        })
        .buffer_unordered(CONCURRENT_SUBMISSIONS);

    let mut submitted_jobs = 0;
    let mut submitted_batches = 0;

    while let Some(result) = submission_stream.next().await {
        match result {
            Ok(Ok(array_size)) => {
                submitted_batches += 1;
                submitted_jobs += array_size;
                if submitted_batches % 10 == 0 {
                    println!("Progress: {}/{} batches submitted ({} jobs)", 
                             submitted_batches, requests_len, submitted_jobs);
                }
            }
            Ok(Err(e)) => {
                eprintln!("❌ Batch submission failed: {:?}", e);
            }
            Err(e) => {
                eprintln!("❌ Task join failed: {:?}", e);
            }
        }
    }

    println!("\n🎉 All {} job batches submitted successfully!", submitted_batches);
    println!("Total jobs submitted: {} (each with {} threads = {} total threads)", submitted_jobs, cpu_per_job - 1, submitted_jobs * (cpu_per_job - 1));
    println!("Expected data generation: ~{}TB across dual bucket architecture", target_tb);
    println!("Submission completed in {:.1}s", start_time.elapsed().as_secs_f64());
    
    Ok(())
}
