use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue};
use rand::Rng;
use anyhow::Result;
use log::{info, warn, error, debug};

pub async fn get_partition_job_order(process_date: &str, dynamodb_client: &DynamoDbClient, table_name: &str, job_id: &str) -> Result<i64> {
    // 1. Check if job already exists in active_jobs
    let get_response = dynamodb_client
        .get_item()
        .table_name(table_name)
        .key("partition_date", AttributeValue::S(process_date.to_string()))
        .send()
        .await?;
        
    if let Some(item) = get_response.item {
        if let Some(active_jobs_attr) = item.get("active_jobs") {
            if let Ok(active_jobs_map) = active_jobs_attr.as_m() {
                if let Some(partition_attr) = active_jobs_map.get(job_id) {
                    if let Ok(partition_str) = partition_attr.as_n() {
                        if let Ok(partition_order) = partition_str.parse::<i64>() {
                            info!("🔄 Job {} restarted, reusing partition {}", job_id, partition_order);
                            return Ok(partition_order);
                        }
                    }
                }
            }
        }
    }
    
    // 2. Job not found, increment counter
    let update_response = dynamodb_client
        .update_item()
        .table_name(table_name)
        .key("partition_date", AttributeValue::S(process_date.to_string()))
        .update_expression("ADD job_counter :inc")
        .expression_attribute_values(":inc", AttributeValue::N("1".to_string()))
        .return_values(ReturnValue::UpdatedNew)
        .send()
        .await?;
        
    let job_counter = update_response
        .attributes()
        .and_then(|attrs| attrs.get("job_counter"))
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<i64>().ok())
        .unwrap_or(1);
    
    let partition_order = job_counter - 1;
    
    // 3. Assign job to partition (handle active_jobs creation)
    let assign_result = dynamodb_client
        .update_item()
        .table_name(table_name)
        .key("partition_date", AttributeValue::S(process_date.to_string()))
        .update_expression("SET active_jobs.#job_id = :partition_order")
        .expression_attribute_names("#job_id", job_id)
        .expression_attribute_values(":partition_order", AttributeValue::N(partition_order.to_string()))
        .send()
        .await;
        
    // If active_jobs doesn't exist, create it first then retry
    if assign_result.is_err() {
        dynamodb_client
            .update_item()
            .table_name(table_name)
            .key("partition_date", AttributeValue::S(process_date.to_string()))
            .update_expression("SET active_jobs = :empty_map")
            .expression_attribute_values(":empty_map", AttributeValue::M(std::collections::HashMap::new()))
            .send()
            .await?;
            
        dynamodb_client
            .update_item()
            .table_name(table_name)
            .key("partition_date", AttributeValue::S(process_date.to_string()))
            .update_expression("SET active_jobs.#job_id = :partition_order")
            .expression_attribute_names("#job_id", job_id)
            .expression_attribute_values(":partition_order", AttributeValue::N(partition_order.to_string()))
            .send()
            .await?;
    }
        
    info!("🆕 Job {} assigned new partition {}", job_id, partition_order);
    Ok(partition_order)
}

pub async fn mark_job_completed(process_date: &str, dynamodb_client: &DynamoDbClient, table_name: &str, job_id: &str) -> Result<()> {
    dynamodb_client
        .update_item()
        .table_name(table_name)
        .key("partition_date", AttributeValue::S(process_date.to_string()))
        .update_expression("REMOVE active_jobs.#job_id")
        .expression_attribute_names("#job_id", job_id)
        .send()
        .await?;
    
    info!("✅ Job {} completed and removed from active_jobs", job_id);
    Ok(())
}

pub async fn _get_random_hash_pan(dynamodb_client: &DynamoDbClient, table_name: &str, thread_id: i32) -> Result<String> {
    // Generate random index from 0-99999 to pick from 100k records stored in our DynamoDB
    let random_index = rand::thread_rng().gen_range(0..100000);
    
    debug!("Thread {} requesting DynamoDB record at index {}", thread_id, random_index);
    
    // Use get_item to retrieve specific record by id
    let result = dynamodb_client
        .get_item()
        .table_name(table_name)
        .key("id", AttributeValue::N(random_index.to_string()))
        .send()
        .await;
    
    match result {
        Ok(output) => {
            if let Some(item) = output.item {
                if let Some(hash_pan_attr) = item.get("hash_pan") {
                    if let Some(hash_pan) = hash_pan_attr.as_s().ok() {
                        info!("✅ Thread {} retrieved hash_pan from DynamoDB (index {})", thread_id, random_index);
                        return Ok(hash_pan.clone());
                    }
                }
            }
            warn!("Thread {} DynamoDB record {} not found, using fallback", thread_id, random_index);
            Ok(format!("hash_{:016x}", rand::thread_rng().gen::<u64>()))
        }
        Err(e) => {
            error!("Thread {} DynamoDB connection failed: {}, using fallback", thread_id, e);
            Ok(format!("hash_{:016x}", rand::thread_rng().gen::<u64>()))
        }
    }
}
pub async fn get_100_random_hash_pans(dynamodb_client: &DynamoDbClient, table_name: &str, thread_id: i32) -> Result<Vec<String>> {
    let mut hash_pans = Vec::with_capacity(1000);

    debug!("Thread {} requesting 1000 random hash_pans from DynamoDB", thread_id);

    for _ in 0..1000 {
        let random_index = rand::thread_rng().gen_range(0..100000);
        
        let result = dynamodb_client
            .get_item()
            .table_name(table_name)
            .key("id", AttributeValue::N(random_index.to_string()))
            .send()
            .await;
        
        match result {
            Ok(output) => {
                if let Some(item) = output.item {
                    if let Some(hash_pan_attr) = item.get("hash_pan") {
                        if let Some(hash_pan) = hash_pan_attr.as_s().ok() {
                            hash_pans.push(hash_pan.clone());
                            continue;
                        }
                    }
                }
                // Fallback if record not found
                hash_pans.push(format!("hash_{:016x}", rand::thread_rng().gen::<u64>()));
            }
            Err(_) => {
                // Fallback if DynamoDB call fails
                hash_pans.push(format!("hash_{:016x}", rand::thread_rng().gen::<u64>()));
            }
        }
    }
    
    info!("✅ Thread {} retrieved {} hash_pans from DynamoDB", thread_id, hash_pans.len());
    Ok(hash_pans)
}
