use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::types::AttributeValue;
use fake::Fake;
use fake::faker::creditcard::en::CreditCardNumber;
use sha2::{Sha256, Digest};
use anyhow::Result;
use std::env;


fn generate_visa_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if clean_pan.starts_with("4") && (clean_pan.len() == 13 || clean_pan.len() == 16 || clean_pan.len() == 19) {
            return clean_pan;
        }
    }
}

fn generate_mastercard_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if clean_pan.starts_with("5") && clean_pan.len() == 16 {
            return clean_pan;
        }
    }
}

fn generate_amex_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if (clean_pan.starts_with("34") || clean_pan.starts_with("37")) && clean_pan.len() == 15 {
            return clean_pan;
        }
    }
}

fn generate_discover_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if clean_pan.starts_with("6") && clean_pan.len() == 16 {
            return clean_pan;
        }
    }
}

fn generate_jcb_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if clean_pan.starts_with("35") && clean_pan.len() == 16 {
            return clean_pan;
        }
    }
}

fn generate_diners_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if (clean_pan.starts_with("30") || clean_pan.starts_with("36") || clean_pan.starts_with("38")) && clean_pan.len() == 14 {
            return clean_pan;
        }
    }
}

fn generate_unionpay_pan() -> String {
    loop {
        let pan: String = CreditCardNumber().fake();
        let clean_pan = pan.replace(" ", "").replace("-", "");
        if clean_pan.starts_with("62") && (clean_pan.len() >= 16 && clean_pan.len() <= 19) {
            return clean_pan;
        }
    }
}

fn generate_random_card_pan() -> (String, String) {
    let card_types = [
        ("Visa", generate_visa_pan as fn() -> String),
        ("Mastercard", generate_mastercard_pan as fn() -> String),
        ("American Express", generate_amex_pan as fn() -> String),
        ("Discover", generate_discover_pan as fn() -> String),
        ("JCB", generate_jcb_pan as fn() -> String),
        ("Diners Club", generate_diners_pan as fn() -> String),
        ("UnionPay", generate_unionpay_pan as fn() -> String),
    ];
    
    let random_index = (0..card_types.len()).fake::<usize>();
    let (card_type, generator) = &card_types[random_index];
    (card_type.to_string(), generator())
}

fn get_card_generator(card_brand: &str) -> Result<(String, fn() -> String)> {
    match card_brand.to_uppercase().as_str() {
        "VISA" => Ok(("Visa".to_string(), generate_visa_pan as fn() -> String)),
        "MASTERCARD" => Ok(("Mastercard".to_string(), generate_mastercard_pan as fn() -> String)),
        "AMEX" | "AMERICAN_EXPRESS" => Ok(("American Express".to_string(), generate_amex_pan as fn() -> String)),
        "DISCOVER" => Ok(("Discover".to_string(), generate_discover_pan as fn() -> String)),
        "JCB" => Ok(("JCB".to_string(), generate_jcb_pan as fn() -> String)),
        "DINERS" | "DINERS_CLUB" => Ok(("Diners Club".to_string(), generate_diners_pan as fn() -> String)),
        "UNIONPAY" => Ok(("UnionPay".to_string(), generate_unionpay_pan as fn() -> String)),
        "MIXED" | "ALL" => Ok(("Mixed".to_string(), || generate_random_card_pan().1)),
        _ => Err(anyhow::anyhow!("Unsupported card brand: {}. Supported: VISA, MASTERCARD, AMEX, DISCOVER, JCB, DINERS, UNIONPAY, MIXED", card_brand))
    }
}

fn hash_pan(pan: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pan.as_bytes());
    hex::encode(hasher.finalize())
}

async fn seed_dynamodb(client: &DynamoDbClient, table_name: &str, card_brand: &str) -> Result<()> {
    let (card_type, generator) = get_card_generator(card_brand)?;
    
    println!("Seeding DynamoDB with 100k {} PANs...", card_type);
    
    let items: Vec<_> = (0..100000)
        .map(|i| {
            let (final_card_type, pan) = if card_type == "Mixed" {
                generate_random_card_pan()
            } else {
                (card_type.clone(), generator())
            };
            let hash_pan = hash_pan(&pan);
            
            let mut item = std::collections::HashMap::new();
            item.insert("id".to_string(), AttributeValue::N(i.to_string()));
            item.insert("pan".to_string(), AttributeValue::S(pan));
            item.insert("hash_pan".to_string(), AttributeValue::S(hash_pan));
            item.insert("card_type".to_string(), AttributeValue::S(final_card_type));
            item
        })
        .collect();

    // Batch write in chunks of 25
    let mut written_count = 0;
    for chunk in items.chunks(25) {
        let write_requests: Vec<_> = chunk
            .iter()
            .map(|item| {
                aws_sdk_dynamodb::types::WriteRequest::builder()
                    .put_request(
                        aws_sdk_dynamodb::types::PutRequest::builder()
                            .set_item(Some(item.clone()))
                            .build()
                            .unwrap()
                    )
                    .build()
            })
            .collect();

        let mut requests_to_process = write_requests;
        
        // Retry unprocessed items
        while !requests_to_process.is_empty() {
            match client
                .batch_write_item()
                .request_items(table_name, requests_to_process.clone())
                .send()
                .await
            {
                Ok(response) => {
                    written_count += requests_to_process.len();
                    
                    // Check for unprocessed items
                    if let Some(unprocessed) = response.unprocessed_items() {
                        if let Some(table_requests) = unprocessed.get(table_name) {
                            requests_to_process = table_requests.clone();
                            println!("Retrying {} unprocessed items...", requests_to_process.len());
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Batch write error: {}, retrying...", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
        
        if written_count % 1000 == 0 {
            println!("Written {} items...", written_count);
        }
    }

    println!("DynamoDB seeding completed!");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let table_name = env::var("HASH_PAN_TABLE_NAME")
        .map_err(|_| anyhow::anyhow!("HASH_PAN_TABLE_NAME env var must be set"))?;
    
    let card_brand = env::var("CARD_BRAND").unwrap_or_else(|_| "MIXED".to_string());

    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = DynamoDbClient::new(&config);

    seed_dynamodb(&client, &table_name, &card_brand).await?;
    Ok(())
}
