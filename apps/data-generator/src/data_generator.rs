use std::sync::Arc;
use arrow::array::{ArrayRef, StringArray, Int64Array, Int32Array, Int16Array, Int8Array, Decimal128Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use crate::common::{
    generate_from_options_with_rng, 
    generate_prefixed_id_with_rng, 
    generate_generic_data_with_rng,
    generate_partition_sequence_number, 
    generate_sha256_hash_with_rng, 
    generate_timestamp_with_rng, 
    generate_insert_timestamp, 
    generate_transaction_business_logic,
    generate_alphanumeric_string_with_rng
};

// Generates data for each row by matching whether the field containes in the table
fn generate_data_for_given_field(
    field_name: &str, 
    row_seed: u64,
    row_index: i64,
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
    chargeback_row_seeds: &[u64], 
    is_chargeback_table: bool,
    process_date: &str, 
    hash_pan_pool: &[String]
) -> String {

    let mut seeded_rng = rand::rngs::StdRng::seed_from_u64(row_seed);
    let mut non_seeded_rng = rand::thread_rng();
    
    // Generate business logic decisions using dedicated function
    let business_logic = generate_transaction_business_logic(&mut seeded_rng);
    
    // Generate shared fields using deterministic seed for consistency across tables
    match field_name {
        
        // Country code groups - merchant group (all same value per row)
        "country_code" | 
        "transaction_country_code" | 
        "merchant_country_code" | 
        "acquirer_country_code" | 
        "processor_country_code" => {
            business_logic.merchant_country_and_currency_map.keys().next().unwrap().clone()
        },

        // Country code groups - issuer group (all same value per row)  
        "issuer_country_code" | 
        "cardholder_country" | 
        "billing_country" | 
        "settlement_country_code" | 
        "clearing_country_code" => {
            business_logic.issuer_country_and_currency_map.keys().next().unwrap().clone()
        },

        // Currency code group (all same value per row, merchant country currency)
        "currency_code" | 
        "original_currency" | 
        "settlement_currency" | 
        "clearing_currency" | 
        // Issuer currency fields - use issuer_country_and_currency_map
        "issuer_currency" | 
        "cardholder_currency" | 
        "billing_currency" => {
            business_logic.issuer_country_and_currency_map.values().next().unwrap().clone()
        },
        "local_currency" => {
            business_logic.merchant_country_and_currency_map.values().next().unwrap().clone()
        },
        name if name.contains("currency") => {
            let currency_options = ["USD", "EUR", "GBP", "CAD", "JPY", "AUD"];
            currency_options[seeded_rng.gen_range(0..currency_options.len())].to_string()
        },
        "sequence_number" => {
            if is_chargeback_table {
                // Extract original row_index from row_seed to match 
                // authorization and clearing sequence numbers for the given row
                let row_index = (row_seed % 100000) as i64;
                generate_partition_sequence_number(row_index, partition_job_order, thread_id, num_threads)
            } else {
                generate_partition_sequence_number(row_index, partition_job_order, thread_id, num_threads)
            }
        },
        "transaction_amount" | "settlement_amount" | "transaction_fee_amount" | "transaction_amount_cents" |
        "interchange_amount_cents" | "handling_amount_cents" | "shipping_amount_cents" | "tip_amount_cents" |
        "chargeback_amount" | "chargeback_amount_cents" | "chargeback_count" |
        "reversal_amount" | "reversal_amount_cents" | "reversal_count" | "refund_amount_cents" |
        "reconciliation_fee" | "reconciliation_fee_processing_code" => {
            
            match field_name {
                "transaction_amount" => format!("{:.2}", business_logic.base_amount),
                "settlement_amount" => format!("{:.2}", business_logic.base_amount),
                "transaction_amount_cents" => {
                    format!("{}", (business_logic.base_amount * 100.0_f64).round() as i64)
                },
                "transaction_fee_amount" => {
                    let fee_rate = seeded_rng.gen_range(0.015..0.035); // 1.5% to 3.5%
                    if field_name == "transaction_fee_amount" {
                        format!("{:.2}", business_logic.base_amount * fee_rate)
                    } else {
                        format!("{}", (business_logic.base_amount * fee_rate * 100.0_f64).round() as i64)
                    }
                },
                "interchange_amount_cents" => {
                    let interchange_rate = seeded_rng.gen_range(0.005..0.025); // 0.5% to 2.5%
                    format!("{}", (business_logic.base_amount * interchange_rate * 100.0) as i64)
                },
                "tip_amount_cents" => {
                    if business_logic.has_tip {
                        let tip_rate = seeded_rng.gen_range(0.10..0.25); // 10% to 25%
                        format!("{}", (business_logic.base_amount * tip_rate * 100.0) as i64)
                    } else {
                        "0".to_string()
                    }
                },
                "shipping_amount_cents" => {
                    if business_logic.has_shipping {
                        let shipping_amount = seeded_rng.gen_range(5.00..50.00);
                        format!("{}", (shipping_amount * 100.0) as i64)
                    } else {
                        "0".to_string()
                    }
                },
                "handling_amount_cents" => {
                    if business_logic.has_handling {
                        let handling_amount = seeded_rng.gen_range(2.00..15.00);
                        format!("{}", (handling_amount * 100.0) as i64)
                    } else {
                        "0".to_string()
                    }
                },
                "chargeback_count" => {
                    // Now using int type (was decimals_38_0)
                    if is_chargeback_table {
                        // Always 1 in chargeback table (each row represents one chargeback)
                        "1".to_string()
                    } else {
                        // Only populate if this row_seed is selected for chargeback table
                        if chargeback_row_seeds.contains(&row_seed) {
                            "1".to_string()
                        } else {
                            "0".to_string()
                        }
                    }
                },
                "chargeback_amount" => {
                    if is_chargeback_table {
                        // Always populate chargeback fields in chargeback table - as decimal
                        format!("{:.2}", business_logic.base_amount * business_logic.chargeback_multiplier)
                    } else {
                        // Only populate if this row_seed is selected for chargeback table
                        if chargeback_row_seeds.contains(&row_seed) {
                            format!("{:.2}", business_logic.base_amount * business_logic.chargeback_multiplier)
                        } else {
                            "0.00".to_string()
                        }
                    }
                },
                "chargeback_amount_cents" => {
                    if is_chargeback_table {
                        // Always populate chargeback fields in chargeback table
                        format!("{}", (business_logic.base_amount * business_logic.chargeback_multiplier * 100.0_f64).round() as i64)
                    } else {
                        // Only populate if this row_seed is selected for chargeback table
                        if chargeback_row_seeds.contains(&row_seed) {
                            format!("{}", (business_logic.base_amount * business_logic.chargeback_multiplier * 100.0_f64).round() as i64)
                        } else {
                            "0".to_string()
                        }
                    }
                },
                "reversal_amount" => {
                    if business_logic.is_reversal && !business_logic.is_auth_declined {
                        // 90% full reversal, 10% partial reversal (technical adjustments)
                        // Now using decimals_38_2 format (was decimals_37_4)
                        if seeded_rng.gen_bool(0.9) {
                            format!("{:.2}", business_logic.base_amount)
                        } else {
                            let partial_rate = seeded_rng.gen_range(0.5..0.95); // 50-95% of original
                            format!("{:.2}", business_logic.base_amount * partial_rate)
                        }
                    } else {
                        "0.00".to_string()
                    }
                },
                "reversal_amount_cents" => {
                    if business_logic.is_reversal && !business_logic.is_auth_declined {
                        // 90% full reversal, 10% partial reversal (technical adjustments)
                        if seeded_rng.gen_bool(0.9) {
                            format!("{}", (business_logic.base_amount * 100.0_f64).round() as i64)
                        } else {
                            let partial_rate = seeded_rng.gen_range(0.5..0.95); // 50-95% of original
                            format!("{}", (business_logic.base_amount * partial_rate * 100.0_f64).round() as i64)
                        }
                    } else {
                        "0".to_string()
                    }
                },
                "reversal_count" => {
                    // Now using int type (was decimals_38_0)
                    if business_logic.is_reversal && !business_logic.is_auth_declined {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                },
                "refund_amount_cents" => {
                    if business_logic.has_refund {
                        // 70% full refund, 30% partial refund
                        if seeded_rng.gen_bool(0.7) {
                            format!("{}", (business_logic.base_amount * 100.0_f64).round() as i64)
                        } else {
                            let partial_rate = seeded_rng.gen_range(0.2..0.8);
                            format!("{}", (business_logic.base_amount * partial_rate * 100.0_f64).round() as i64)
                        }
                    } else {
                        "0".to_string()
                    }
                },
                "reconciliation_fee" => {
                    if business_logic.has_reconciliation_fee {
                        let reconciliation_amount = seeded_rng.gen_range(1.00..25.00);
                        format!("{:.2}", reconciliation_amount)
                    } else {
                        "0.00".to_string()
                    }
                },
                "reconciliation_fee_processing_code" => {
                    if business_logic.has_reconciliation_fee {
                        generate_from_options_with_rng(&["REC001", "REC002", "REC003", "ADJ001", "ADJ002"], &mut seeded_rng)
                    } else {
                        "".to_string()
                    }
                },
                _ => unreachable!()
            }
        },
        "issuer_rate" => {
            format!("{:.4}", business_logic.issuer_rate)
        },
        "network_rate" => {
            format!("{:.4}", business_logic.network_rate)
        },
        "risk_rate" => {
            format!("{:.4}", business_logic.risk_rate)
        },
        "acquirer_rate" => {
            format!("{:.4}", business_logic.acquirer_rate)
        },
        "exchange_rate" => {
            format!("{:.4}", business_logic.exchange_rate)
        },
        "interchange_rate" => {
            format!("{:.4}", business_logic.interchange_rate)
        },
        "processing_rate" => {
            format!("{:.4}", business_logic.processing_rate)
        },
        "daily_transaction_count" => {
            "1".to_string()
        },
        "transaction_type" => {
            business_logic.transaction_type.clone()
        },
        "transaction_type_cd" => {
            match business_logic.transaction_type.as_str() {
                "PURCHASE" => "00",
                "CASH_ADVANCE" => "01",
                "REFUND" => "20",
                "BALANCE_TRANSFER" => "40",
                "ADJUSTMENT" => "92",
                "FEE" => "28",
                _ => "00"
            }.to_string()
        },
        "transaction_status_code" => {
            if business_logic.is_auth_declined {
                "5" // Declined
            } else if business_logic.is_reversal {
                "3" // Reversed
            } else if business_logic.has_refund {
                "4" // Refunded
            } else if chargeback_row_seeds.contains(&row_seed) || is_chargeback_table {
                "6" // Chargeback
            } else {
                "0" // Approved/Completed
            }.to_string()
        },
        
        "account_age_indicator" => generate_from_options_with_rng(&["01", "02", "03", "04", "05"], &mut seeded_rng),
        "account_change_indicator" => generate_from_options_with_rng(&["01", "02", "03", "04"], &mut seeded_rng),
        "account_info" => generate_generic_data_with_rng("account_info", &mut seeded_rng),
        "account_pwd_change_indicator" => generate_from_options_with_rng(&["01", "02", "03", "04"], &mut seeded_rng),
        "acquirer_id" => generate_prefixed_id_with_rng("ACQ", 8, &mut seeded_rng),
        "acs_transaction_id" => generate_prefixed_id_with_rng("ACS", 32, &mut seeded_rng),
        "address_reputation" => generate_from_options_with_rng(&["GOOD", "POOR", "UNKNOWN"], &mut seeded_rng),
        "alert_pattern" => {
            if business_logic.is_auth_declined {
                "HIGH_RISK".to_string()
            } else if business_logic.base_amount > 5000.0 {
                "SUSPICIOUS".to_string()
            } else {
                match business_logic.transaction_type.as_str() {
                    "CASH_ADVANCE" => if business_logic.base_amount > 1000.0 { "SUSPICIOUS" } else { "NORMAL" },
                    _ => "NORMAL"
                }.to_string()
            }
        },
        "auth_data" => generate_prefixed_id_with_rng("AUTH", 16, &mut seeded_rng),
        "auth_method" => generate_from_options_with_rng(&["PASSWORD", "BIOMETRIC", "TOKEN", "SMS"], &mut seeded_rng),
        "authentication_status" => generate_from_options_with_rng(&["Y", "N", "A", "U", "R"], &mut seeded_rng),
        "batch_id" => generate_prefixed_id_with_rng("BATCH", 12, &mut seeded_rng),
        "bill_addr_city" | "bill_addr_country" | "bill_addr_line" | "bill_addr_post_code" | "bill_addr_state" => {
            let country = business_logic.issuer_country_and_currency_map.keys().next().unwrap().as_str();
            
            match field_name {
                "bill_addr_country" => country.to_string(),
                "bill_addr_city" => {
                    match country {
                "USA" => ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][seeded_rng.gen_range(0..5)],
                "CAN" => ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"][seeded_rng.gen_range(0..5)],
                "GBR" => ["London", "Manchester", "Birmingham", "Liverpool", "Leeds"][seeded_rng.gen_range(0..5)],
                "DEU" => ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"][seeded_rng.gen_range(0..5)],
                "FRA" => ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"][seeded_rng.gen_range(0..5)],
                "AUS" => ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"][seeded_rng.gen_range(0..5)],
                "JPN" => ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"][seeded_rng.gen_range(0..5)],
                "ITA" => ["Rome", "Milan", "Naples", "Turin", "Florence"][seeded_rng.gen_range(0..5)],
                "ESP" => ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"][seeded_rng.gen_range(0..5)],
                "NLD" => ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"][seeded_rng.gen_range(0..5)],
                "BEL" => ["Brussels", "Antwerp", "Ghent", "Charleroi", "Liege"][seeded_rng.gen_range(0..5)],
                "CHE" => ["Zurich", "Geneva", "Basel", "Bern", "Lausanne"][seeded_rng.gen_range(0..5)],
                "AUT" => ["Vienna", "Salzburg", "Innsbruck", "Graz", "Linz"][seeded_rng.gen_range(0..5)],
                "SWE" => ["Stockholm", "Gothenburg", "Malmo", "Uppsala", "Vasteras"][seeded_rng.gen_range(0..5)],
                "NOR" => ["Oslo", "Bergen", "Trondheim", "Stavanger", "Drammen"][seeded_rng.gen_range(0..5)],
                "DNK" => ["Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg"][seeded_rng.gen_range(0..5)],
                "FIN" => ["Helsinki", "Espoo", "Tampere", "Vantaa", "Turku"][seeded_rng.gen_range(0..5)],
                "IRL" => ["Dublin", "Cork", "Limerick", "Galway", "Waterford"][seeded_rng.gen_range(0..5)],
                "PRT" => ["Lisbon", "Porto", "Vila Nova de Gaia", "Amadora", "Braga"][seeded_rng.gen_range(0..5)],
                "GRC" => ["Athens", "Thessaloniki", "Patras", "Heraklion", "Larissa"][seeded_rng.gen_range(0..5)],
                "POL" => ["Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan"][seeded_rng.gen_range(0..5)],
                "CZE" => ["Prague", "Brno", "Ostrava", "Plzen", "Liberec"][seeded_rng.gen_range(0..5)],
                "HUN" => ["Budapest", "Debrecen", "Szeged", "Miskolc", "Pecs"][seeded_rng.gen_range(0..5)],
                "SVK" => ["Bratislava", "Kosice", "Presov", "Zilina", "Banska Bystrica"][seeded_rng.gen_range(0..5)],
                "SVN" => ["Ljubljana", "Maribor", "Celje", "Kranj", "Velenje"][seeded_rng.gen_range(0..5)],
                "EST" => ["Tallinn", "Tartu", "Narva", "Parnu", "Kohtla-Jarve"][seeded_rng.gen_range(0..5)],
                "LVA" => ["Riga", "Daugavpils", "Liepaja", "Jelgava", "Jurmala"][seeded_rng.gen_range(0..5)],
                "LTU" => ["Vilnius", "Kaunas", "Klaipeda", "Siauliai", "Panevezys"][seeded_rng.gen_range(0..5)],
                "BGR" => ["Sofia", "Plovdiv", "Varna", "Burgas", "Ruse"][seeded_rng.gen_range(0..5)],
                "ROU" => ["Bucharest", "Cluj-Napoca", "Timisoara", "Iasi", "Constanta"][seeded_rng.gen_range(0..5)],
                "HRV" => ["Zagreb", "Split", "Rijeka", "Osijek", "Zadar"][seeded_rng.gen_range(0..5)],
                "MEX" => ["Mexico City", "Guadalajara", "Monterrey", "Puebla", "Tijuana"][seeded_rng.gen_range(0..5)],
                "BRA" => ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"][seeded_rng.gen_range(0..5)],
                "ARG" => ["Buenos Aires", "Cordoba", "Rosario", "Mendoza", "La Plata"][seeded_rng.gen_range(0..5)],
                "CHL" => ["Santiago", "Valparaiso", "Concepcion", "La Serena", "Antofagasta"][seeded_rng.gen_range(0..5)],
                "COL" => ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena"][seeded_rng.gen_range(0..5)],
                "PER" => ["Lima", "Arequipa", "Trujillo", "Chiclayo", "Huancayo"][seeded_rng.gen_range(0..5)],
                "VEN" => ["Caracas", "Maracaibo", "Valencia", "Barquisimeto", "Maracay"][seeded_rng.gen_range(0..5)],
                "URY" => ["Montevideo", "Salto", "Paysandu", "Las Piedras", "Rivera"][seeded_rng.gen_range(0..5)],
                "PRY" => ["Asuncion", "Ciudad del Este", "San Lorenzo", "Luque", "Capiata"][seeded_rng.gen_range(0..5)],
                "BOL" => ["La Paz", "Santa Cruz", "Cochabamba", "Sucre", "Oruro"][seeded_rng.gen_range(0..5)],
                "ECU" => ["Quito", "Guayaquil", "Cuenca", "Santo Domingo", "Machala"][seeded_rng.gen_range(0..5)],
                "GUY" => ["Georgetown", "Linden", "New Amsterdam", "Anna Regina", "Bartica"][seeded_rng.gen_range(0..5)],
                "SUR" => ["Paramaribo", "Lelydorp", "Brokopondo", "Nieuw Nickerie", "Moengo"][seeded_rng.gen_range(0..5)],
                "GUF" => ["Cayenne", "Saint-Laurent-du-Maroni", "Kourou", "Remire-Montjoly", "Matoury"][seeded_rng.gen_range(0..5)],
                _ => ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][seeded_rng.gen_range(0..5)]
            }.to_string()
                },
                "bill_addr_line" => format!("{} Billing St", seeded_rng.gen_range(100..9999)),
                "bill_addr_post_code" => {
                    match country {
                        "USA" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "CAN" => format!("{}{}{} {}{}{}", 
                            ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'X', 'Y'][seeded_rng.gen_range(0..18)] as char,
                            seeded_rng.gen_range(0..10),
                            ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..20)] as char,
                            seeded_rng.gen_range(0..10),
                            ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..20)] as char,
                            seeded_rng.gen_range(0..10)
                        ),
                        "GBR" => format!("{}{} {}{}{}", 
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..25)] as char,
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..25)] as char,
                            seeded_rng.gen_range(0..10),
                            seeded_rng.gen_range(0..10),
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..25)] as char
                        ),
                        "DEU" | "AUT" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "FRA" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "AUS" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "JPN" => format!("{:03}-{:04}", seeded_rng.gen_range(100..999), seeded_rng.gen_range(1000..9999)),
                        "ITA" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "ESP" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "NLD" => format!("{:04} {}{}", seeded_rng.gen_range(1000..9999), 
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Z'][seeded_rng.gen_range(0..21)] as char,
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Z'][seeded_rng.gen_range(0..21)] as char
                        ),
                        "BEL" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "CHE" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "SWE" => format!("{:03} {:02}", seeded_rng.gen_range(100..999), seeded_rng.gen_range(10..99)),
                        "NOR" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "DNK" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "FIN" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "BRA" => format!("{:05}-{:03}", seeded_rng.gen_range(10000..99999), seeded_rng.gen_range(100..999)),
                        "MEX" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "ARG" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "CHL" => format!("{:07}", seeded_rng.gen_range(1000000..9999999)),
                        "COL" => format!("{:06}", seeded_rng.gen_range(100000..999999)),
                        "PER" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "URY" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "ECU" => format!("{:06}", seeded_rng.gen_range(100000..999999)),
                        "IRL" => format!("{}{}{} {}{}{}{}", 
                            ['A', 'C', 'D', 'E', 'F', 'H', 'K', 'N', 'P', 'R', 'T', 'V', 'W', 'X', 'Y'][seeded_rng.gen_range(0..15)] as char,
                            seeded_rng.gen_range(10..99),
                            ['A', 'C', 'D', 'E', 'F', 'H', 'K', 'N', 'P', 'R', 'T', 'V', 'W', 'X', 'Y'][seeded_rng.gen_range(0..15)] as char,
                            ['A', 'C', 'D', 'E', 'F', 'H', 'K', 'N', 'P', 'R', 'T', 'V', 'W', 'X', 'Y'][seeded_rng.gen_range(0..15)] as char,
                            seeded_rng.gen_range(10..99),
                            ['A', 'C', 'D', 'E', 'F', 'H', 'K', 'N', 'P', 'R', 'T', 'V', 'W', 'X', 'Y'][seeded_rng.gen_range(0..15)] as char,
                            seeded_rng.gen_range(10..99)
                        ),
                        "PRT" => format!("{:04}-{:03}", seeded_rng.gen_range(1000..9999), seeded_rng.gen_range(100..999)),
                        "POL" => format!("{:02}-{:03}", seeded_rng.gen_range(10..99), seeded_rng.gen_range(100..999)),
                        "CZE" => format!("{:03} {:02}", seeded_rng.gen_range(100..999), seeded_rng.gen_range(10..99)),
                        "HUN" => format!("{:04}", seeded_rng.gen_range(1000..9999)),
                        "SVK" => format!("{:03} {:02}", seeded_rng.gen_range(100..999), seeded_rng.gen_range(10..99)),
                        _ => format!("{:05}", seeded_rng.gen_range(10000..99999))
                    }.to_string()
                },
                "bill_addr_state" => {
                    match country {
                        "USA" => ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"][seeded_rng.gen_range(0..10)],
                        "CAN" => ["ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE"][seeded_rng.gen_range(0..10)],
                        "AUS" => ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"][seeded_rng.gen_range(0..8)],
                        "MEX" => ["CDMX", "JAL", "NL", "PUE", "BC", "VER", "GTO", "MICH", "CHIH", "OAX"][seeded_rng.gen_range(0..10)],
                        "BRA" => ["SP", "RJ", "MG", "BA", "PR", "RS", "PE", "CE", "PA", "SC"][seeded_rng.gen_range(0..10)],
                        "ARG" => ["BA", "CABA", "COR", "SF", "MEN", "TUC", "ENT", "CHA", "COR", "MIS"][seeded_rng.gen_range(0..10)],
                        "DEU" => ["BY", "BW", "NW", "NI", "HE", "SN", "RP", "TH", "SH", "HH"][seeded_rng.gen_range(0..10)],
                        "ITA" => ["LOM", "LAZ", "CAM", "SIC", "VEN", "EMR", "PIE", "PUG", "TOS", "CAL"][seeded_rng.gen_range(0..10)],
                        "ESP" => ["AND", "CAT", "MAD", "VAL", "GAL", "CAS", "PVA", "CAN", "MUR", "EXT"][seeded_rng.gen_range(0..10)],
                        "COL" => ["BOG", "ANT", "VAL", "ATL", "SAN", "BOL", "CUN", "NOR", "COR", "HUI"][seeded_rng.gen_range(0..10)],
                        "PER" => ["LIM", "ARE", "LAL", "LAM", "CUS", "JUN", "PIU", "ANC", "HUC", "ICA"][seeded_rng.gen_range(0..10)],
                        "VEN" => ["CAR", "ZUL", "CAR", "LAR", "ARA", "BOL", "TAC", "MER", "FAL", "SUC"][seeded_rng.gen_range(0..10)],
                        _ => "N/A"
                    }.to_string()
                },
                _ => unreachable!()
            }
        },
        "browser_info" => generate_from_options_with_rng(&["Chrome", "Safari", "Firefox", "Edge"], &mut seeded_rng),
        "card_brand" => std::env::var("CARD_BRAND").unwrap_or_else(|_| "MASTERCARD".to_string()),
        "hash_pan" => {
            // Use seeded_rng to consistently select from hash_pan_pool for same row_seed
            let index = seeded_rng.gen_range(0..hash_pan_pool.len());
            hash_pan_pool[index].clone()
        },
        "card_product_id" => {
            let card_brand = std::env::var("CARD_BRAND").unwrap_or_else(|_| "MASTERCARD".to_string());
            match card_brand.as_str() {
                "VISA" => generate_from_options_with_rng(&[
                    "CSP", "CSR", "CFU", "CFF", "IHG", "UAX", // Chase Visa cards
                    "CCR", "TRV", "PRM", "MLB", // Bank of America Visa
                    "ACT", "AUT", "REF", // Wells Fargo Visa
                    "VTX", "VT1", "QS1", "SAV"  // Capital One Visa
                ], &mut seeded_rng),
                "MASTERCARD" => generate_from_options_with_rng(&[
                    "CFX", "WOH", "BUS", // Chase Mastercard (Freedom Flex, World of Hyatt, Business)
                    "CUS", "SEC", "BIZ", // Bank of America Mastercard
                    "BZP", "SIG", "PLT", // Wells Fargo Mastercard
                    "SPK", "QSL", "VEN"  // Capital One Mastercard
                ], &mut seeded_rng),
                "AMEX" | "AMERICAN_EXPRESS" => generate_from_options_with_rng(&[
                    "PLT", "GLD", "GRN", "BBP", "SPG", "HLT", "DLT", "BCP"
                ], &mut seeded_rng),
                "DISCOVER" => generate_from_options_with_rng(&[
                    "IT1", "IT2", "CSH", "STU", "SEC", "CHR"
                ], &mut seeded_rng),
                _ => generate_from_options_with_rng(&[
                    "PLT", "SIL", "BRZ", "DMN"
                ], &mut seeded_rng)
            }
        },
        "card_type" => generate_from_options_with_rng(&["CREDIT", "DEBIT", "PREPAID"], &mut seeded_rng),
        "cardholder_name_hash" => generate_sha256_hash_with_rng(&mut seeded_rng),
        "cardholder_present_code" => generate_from_options_with_rng(&["0", "1", "2", "5"], &mut seeded_rng),
        "card_present_code" => generate_from_options_with_rng(&["0", "1", "2", "5"], &mut seeded_rng),
        "cavv_result" => generate_from_options_with_rng(&["0", "1", "2", "3", "4"], &mut seeded_rng),
        "ccpa_pattern" => generate_from_options_with_rng(&["COMPLIANT", "NON_COMPLIANT", "EXEMPT"], &mut seeded_rng),
        "channel_type" => generate_from_options_with_rng(&["ONLINE", "MOBILE", "POS", "ATM"], &mut seeded_rng),
        "clearing_network" => std::env::var("NETWORK_BRAND").unwrap_or_else(|_| "MASTERCARD".to_string()),
        "cryptogram_type" => generate_from_options_with_rng(&["ARQC", "TC", "AAC", "CDA"], &mut seeded_rng),
        "customer_id" => generate_prefixed_id_with_rng("CUST", 12, &mut seeded_rng),
        "device_channel" => generate_from_options_with_rng(&["01", "02", "03"], &mut seeded_rng),
        "device_fingerprint" => generate_prefixed_id_with_rng("FP", 32, &mut seeded_rng),
        "directory_server_id" => generate_prefixed_id_with_rng("DS", 16, &mut seeded_rng),
        "eci_indicator" => generate_from_options_with_rng(&["05", "06", "07", "02"], &mut seeded_rng),
        "enrollment_status" => generate_from_options_with_rng(&["Y", "N", "U"], &mut seeded_rng),
        "expiry_date" => {
            let year = seeded_rng.gen_range(2026..2031);
            let month = seeded_rng.gen_range(1..13);
            format!("{:02}/{}", month, year % 100)
        },
        "geolocation_result" => generate_from_options_with_rng(&["MATCH", "NO_MATCH", "UNAVAILABLE"], &mut seeded_rng),
        "interchange_category" => generate_from_options_with_rng(&["STANDARD", "ENHANCED", "PREMIUM"], &mut seeded_rng),
        "ip_address" => format!("{}.{}.{}.{}", seeded_rng.gen_range(1..255), seeded_rng.gen_range(0..255), seeded_rng.gen_range(0..255), seeded_rng.gen_range(1..255)),
        "issuer_id" => generate_prefixed_id_with_rng("ISS", 8, &mut seeded_rng),

        "merchant_name" | 
        "merchant_dba" | 
        "merchant_legal_name" | 
        "merchant_category_code" | 
        "merchant_code" | 
        "business_region_code" |
        "merchant_id" => {
            // Get the selected merchant country from business logic
            let selected_country = business_logic.merchant_country_and_currency_map.keys().next().unwrap();
            
            // Get merchants for that country
            let merchants = match selected_country.as_str() {

                // === NORTH AMERICA REGION (001) ===
                // United States - Major retail chains, tech companies, and department stores
                "USA" => vec![
                    ("Amazon", "Amazon.com", "Amazon.com Inc", "5999", "001", "MID001234567890"),
                    ("Walmart", "Walmart", "Walmart Inc", "5411", "001", "MID002345678901"),
                    ("Target", "Target", "Target Corporation", "5331", "001", "MID003456789012"),
                    ("Costco", "Costco", "Costco Wholesale Corporation", "5300", "001", "MID004567890123"),
                    ("Home Depot", "Home Depot", "The Home Depot Inc", "5211", "001", "MID005678901234"),
                    ("Starbucks", "Starbucks", "Starbucks Corporation", "5814", "001", "MID006789012345"),
                    ("McDonald's", "McDonald's", "McDonald's Corporation", "5814", "001", "MID007890123456"),
                    ("Apple Store", "Apple Store", "Apple Inc", "5732", "001", "MID008901234567"),
                    ("Best Buy", "Best Buy", "Best Buy Co Inc", "5732", "001", "MID009012345678"),
                    ("Macy's", "Macy's", "Macy's Inc", "5311", "001", "MID010123456789"),
                    ("CVS Pharmacy", "CVS", "CVS Health Corporation", "5912", "001", "MID011234567890"),
                    ("Walgreens", "Walgreens", "Walgreens Boots Alliance", "5912", "001", "MID012345678901"),
                    ("Nike", "Nike Store", "Nike Inc", "5655", "001", "MID013456789012"),
                    ("Gap", "Gap", "The Gap Inc", "5651", "001", "MID014567890123"),
                    ("Whole Foods", "Whole Foods Market", "Amazon.com Inc", "5411", "001", "MID015678901234"),
                    ("Kroger", "Kroger", "The Kroger Co", "5411", "001", "MID016789012345"),
                    ("Lowe's", "Lowe's", "Lowe's Companies Inc", "5211", "001", "MID017890123456"),
                    ("Nordstrom", "Nordstrom", "Nordstrom Inc", "5311", "001", "MID018901234567"),
                    ("Kohl's", "Kohl's", "Kohl's Corporation", "5311", "001", "MID019012345678"),
                    ("GameStop", "GameStop", "GameStop Corp", "5734", "001", "MID020123456789"),
                    ("Barnes & Noble", "Barnes & Noble", "Barnes & Noble Inc", "5942", "001", "MID021234567890"),
                    ("Bed Bath & Beyond", "Bed Bath & Beyond", "Bed Bath & Beyond Inc", "5712", "001", "MID022345678901"),
                    ("TJ Maxx", "TJ Maxx", "The TJX Companies Inc", "5651", "001", "MID023456789012"),
                    ("Marshalls", "Marshalls", "The TJX Companies Inc", "5651", "001", "MID024567890123"),
                    ("Old Navy", "Old Navy", "The Gap Inc", "5651", "001", "MID025678901234"),
                ],

                // Canada - Major Canadian retail chains and franchises
                "CAN" => vec![
                    ("Tim Hortons", "Tim Hortons", "Tim Hortons Inc", "5814", "001", "MID026789012345"),
                    ("Canadian Tire", "Canadian Tire", "Canadian Tire Corporation", "5531", "001", "MID027890123456"),
                    ("Loblaws", "Loblaws", "Loblaw Companies Limited", "5411", "001", "MID028901234567"),
                    ("Shoppers Drug Mart", "Shoppers", "Shoppers Drug Mart Corporation", "5912", "001", "MID029012345678"),
                    ("Metro", "Metro", "Metro Inc", "5411", "001", "MID030123456789"),
                    ("Hudson's Bay", "The Bay", "Hudson's Bay Company", "5311", "001", "MID031234567890"),
                    ("Sobeys", "Sobeys", "Empire Company Limited", "5411", "001", "MID032345678901"),
                    ("Costco Canada", "Costco", "Costco Wholesale Canada Ltd", "5300", "001", "MID033456789012"),
                    ("Walmart Canada", "Walmart", "Walmart Canada Corp", "5411", "001", "MID034567890123"),
                    ("Home Depot Canada", "Home Depot", "The Home Depot of Canada Inc", "5211", "001", "MID035678901234"),
                    ("Best Buy Canada", "Best Buy", "Best Buy Canada Ltd", "5732", "001", "MID036789012345"),
                    ("Rona", "Rona", "Rona Inc", "5211", "001", "MID037890123456"),
                    ("Winners", "Winners", "TJX Canada", "5651", "001", "MID038901234567"),
                    ("Sport Chek", "Sport Chek", "Fgl Sports Ltd", "5655", "001", "MID039012345678"),
                ],

                // === EUROPE REGION (002) ===
                // United Kingdom - Major British retail chains and department stores
                "GBR" => vec![
                    ("Tesco", "Tesco", "Tesco PLC", "5411", "002", "MID040123456789"),
                    ("Sainsbury's", "Sainsbury's", "J Sainsbury plc", "5411", "002", "MID041234567890"),
                    ("John Lewis", "John Lewis", "John Lewis Partnership", "5311", "002", "MID042345678901"),
                    ("Marks & Spencer", "M&S", "Marks and Spencer Group plc", "5311", "002", "MID043456789012"),
                    ("ASDA", "ASDA", "ASDA Group Limited", "5411", "002", "MID044567890123"),
                    ("Boots", "Boots", "Walgreens Boots Alliance", "5912", "002", "MID045678901234"),
                    ("Argos", "Argos", "Sainsbury's Argos", "5399", "002", "MID046789012345"),
                ],

                // France - Major French retail chains and hypermarkets
                "FRA" => vec![
                    ("Carrefour", "Carrefour", "Carrefour SA", "5411", "002", "MID047890123456"),
                    ("Leclerc", "Leclerc", "E.Leclerc", "5411", "002", "MID048901234567"),
                    ("Galeries Lafayette", "Galeries Lafayette", "Groupe Galeries Lafayette", "5311", "002", "MID049012345678"),
                    ("Auchan", "Auchan", "Groupe Auchan", "5411", "002", "MID050123456789"),
                    ("Monoprix", "Monoprix", "Groupe Casino", "5411", "002", "MID051234567890"),
                ],

                // Germany - Major German retail chains and e-commerce platforms
                "DEU" => vec![
                    ("REWE", "REWE", "REWE Group", "5411", "002", "MID052345678901"),
                    ("Lidl", "Lidl", "Lidl Stiftung & Co KG", "5411", "002", "MID053456789012"),
                    ("MediaMarkt", "MediaMarkt", "MediaMarkt Saturn Retail Group", "5732", "002", "MID054567890123"),
                    ("Zalando", "Zalando", "Zalando SE", "5651", "002", "MID055678901234"),
                    ("Edeka", "Edeka", "Edeka Zentrale AG", "5411", "002", "MID056789012345"),
                    ("Aldi", "Aldi", "ALDI Group", "5411", "002", "MID057890123456"),
                ],

                // Spain - Major Spanish retail chains and fashion brands
                "ESP" => vec![
                    ("Zara", "Zara", "Inditex SA", "5651", "002", "MID058901234567"),
                    ("El Corte Inglés", "El Corte Inglés", "El Corte Inglés SA", "5311", "002", "MID059012345678"),
                    ("Mercadona", "Mercadona", "Mercadona SA", "5411", "002", "MID060123456789"),
                    ("Mango", "Mango", "Punto Fa SL", "5651", "002", "MID061234567890"),
                ],

                // Italy - Major Italian retail chains and cooperatives
                "ITA" => vec![
                    ("Esselunga", "Esselunga", "Esselunga SpA", "5411", "002", "MID062345678901"),
                    ("Coop Italia", "Coop", "Coop Italia", "5411", "002", "MID063456789012"),
                    ("Conad", "Conad", "Conad Consorzio Nazionale", "5411", "002", "MID064567890123"),
                ],

                // Netherlands - Major Dutch retail chains and e-commerce
                "NLD" => vec![
                    ("Albert Heijn", "Albert Heijn", "Koninklijke Ahold Delhaize NV", "5411", "002", "MID065678901234"),
                    ("Jumbo", "Jumbo", "Jumbo Groep Holding BV", "5411", "002", "MID066789012345"),
                    ("Bol.com", "Bol.com", "Bol.com BV", "5999", "002", "MID067890123456"),
                ],

                // Sweden - Major Swedish retail chains and global brands
                "SWE" => vec![
                    ("H&M", "H&M", "H&M Hennes & Mauritz AB", "5651", "002", "MID068901234567"),
                    ("IKEA", "IKEA", "IKEA Group", "5712", "002", "MID069012345678"),
                    ("ICA", "ICA", "ICA Gruppen AB", "5411", "002", "MID070123456789"),
                    ("Coop Sweden", "Coop", "KF Gruppen", "5411", "002", "MID071234567890"),
                ],

                // Switzerland - Major Swiss retail chains and cooperatives
                "CHE" => vec![
                    ("Migros", "Migros", "Migros-Genossenschafts-Bund", "5411", "002", "MID072345678901"),
                    ("Coop Switzerland", "Coop", "Coop Group", "5411", "002", "MID073456789012"),
                ],

                // === ASIA PACIFIC REGION (003) ===
                // Japan - Major Japanese convenience stores and retail chains
                "JPN" => vec![
                    ("7-Eleven Japan", "7-Eleven", "Seven & i Holdings Co", "5499", "003", "MID074567890123"),
                    ("Uniqlo", "Uniqlo", "Fast Retailing Co Ltd", "5651", "003", "MID075678901234"),
                    ("Lawson", "Lawson", "Lawson Inc", "5499", "003", "MID076789012345"),
                    ("Don Quijote", "Don Quijote", "Pan Pacific International Holdings", "5399", "003", "MID077890123456"),
                    ("FamilyMart", "FamilyMart", "FamilyMart Co Ltd", "5499", "003", "MID078901234567"),
                    ("Aeon", "Aeon", "Aeon Co Ltd", "5411", "003", "MID079012345678"),
                ],

                // Australia - Major Australian retail chains and electronics stores
                "AUS" => vec![
                    ("Coles", "Coles", "Coles Group Limited", "5411", "003", "MID080123456789"),
                    ("Woolworths", "Woolworths", "Woolworths Group Limited", "5411", "003", "MID081234567890"),
                    ("JB Hi-Fi", "JB Hi-Fi", "JB Hi-Fi Limited", "5732", "003", "MID082345678901"),
                    ("Bunnings", "Bunnings", "Bunnings Group Limited", "5211", "003", "MID083456789012"),
                    ("Harvey Norman", "Harvey Norman", "Harvey Norman Holdings Limited", "5732", "003", "MID084567890123"),
                ],

                // Singapore - Major Singaporean retail chains and financial services
                "SGP" => vec![
                    ("NTUC FairPrice", "FairPrice", "NTUC FairPrice Co-operative Ltd", "5411", "003", "MID085678901234"),
                    ("DBS Bank", "DBS", "DBS Bank Ltd", "6011", "003", "MID086789012345"),
                    ("Cold Storage", "Cold Storage", "Dairy Farm International", "5411", "003", "MID087890123456"),
                ],

                // South Korea - Major Korean retail chains and department stores
                "KOR" => vec![
                    ("Lotte Mart", "Lotte Mart", "Lotte Shopping Co Ltd", "5411", "003", "MID088901234567"),
                    ("E-Mart", "E-Mart", "Shinsegae Group", "5411", "003", "MID089012345678"),
                    ("Homeplus", "Homeplus", "Homeplus Co Ltd", "5411", "003", "MID090123456789"),
                ],

                // === LATIN AMERICA REGION (004) ===
                // Brazil - Major Brazilian retail chains and e-commerce platforms
                "BRA" => vec![
                    ("Pão de Açúcar", "Pão de Açúcar", "Grupo Pão de Açúcar", "5411", "004", "MID091234567890"),
                    ("Magazine Luiza", "Magalu", "Magazine Luiza SA", "5732", "004", "MID092345678901"),
                    ("Carrefour Brasil", "Carrefour", "Carrefour Brasil", "5411", "004", "MID093456789012"),
                    ("Americanas", "Americanas", "Americanas SA", "5399", "004", "MID094567890123"),
                ],

                // Mexico - Major Mexican retail chains and department stores
                "MEX" => vec![
                    ("Soriana", "Soriana", "Organización Soriana SAB", "5411", "004", "MID095678901234"),
                    ("Liverpool", "Liverpool", "El Puerto de Liverpool", "5311", "004", "MID096789012345"),
                    ("Walmart Mexico", "Walmart", "Walmart de México", "5411", "004", "MID097890123456"),
                ],

                // Argentina - Major Argentine retail chains and e-commerce
                "ARG" => vec![
                    ("Mercado Libre", "MercadoLibre", "MercadoLibre Inc", "5999", "004", "MID098901234567"),
                    ("Coto", "Coto", "Coto CICSA", "5411", "004", "MID099012345678"),
                    ("Jumbo Argentina", "Jumbo", "Cencosud SA", "5411", "004", "MID100123456789"),
                ],

                // Chile - Major Chilean retail chains and department stores
                "CHL" => vec![
                    ("Falabella", "Falabella", "S.A.C.I. Falabella", "5311", "004", "MID101234567890"),
                    ("Lider", "Lider", "Walmart Chile", "5411", "004", "MID102345678901"),
                    ("Jumbo Chile", "Jumbo", "Cencosud SA", "5411", "004", "MID103456789012"),
                ],

                // Colombia - Major Colombian retail chains and supermarkets
                "COL" => vec![
                    ("Éxito", "Éxito", "Grupo Éxito", "5411", "004", "MID104567890123"),
                    ("Carulla", "Carulla", "Grupo Éxito", "5411", "004", "MID105678901234"),
                ],

                // Peru - Major Peruvian retail chains and supermarkets
                "PER" => vec![
                    ("Ripley", "Ripley", "Ripley Corp SA", "5311", "004", "MID106789012345"),
                    ("Wong", "Wong", "Cencosud SA", "5411", "004", "MID107890123456"),
                    ("Plaza Vea", "Plaza Vea", "Supermercados Peruanos SA", "5411", "004", "MID108901234567"),
                    ("Vea", "Vea", "Supermercados Peruanos SA", "5411", "004", "MID109012345678"),
                ],

                // === MIDDLE EAST & AFRICA REGION (005) ===
                // United Arab Emirates - Major UAE retail chains and hypermarkets
                "ARE" => vec![
                    ("Carrefour UAE", "Carrefour", "Majid Al Futtaim Retail", "5411", "005", "MID110123456789"),
                    ("Lulu Hypermarket", "Lulu", "Lulu Group International", "5411", "005", "MID111234567890"),
                    ("Spinneys", "Spinneys", "Spinneys LLC", "5411", "005", "MID112345678901"),
                    ("Al Tayer", "Al Tayer", "Al Tayer Group", "5311", "005", "MID113456789012"),
                ],

                // South Africa - Major South African retail chains and department stores
                "ZAF" => vec![
                    ("Pick n Pay", "Pick n Pay", "Pick n Pay Stores Ltd", "5411", "005", "MID114567890123"),
                    ("Shoprite", "Shoprite", "Shoprite Holdings Ltd", "5411", "005", "MID115678901234"),
                    ("Woolworths SA", "Woolworths", "Woolworths Holdings Limited", "5311", "005", "MID116789012345"),
                ],
                _ => vec![("Global Store", "Global Store", "Global Retail Inc", "5999", "001", "MID999999999999")], // Fallback
            };
            
            let merchant_index = seeded_rng.gen_range(0..merchants.len());
            let (name, dba, legal_name, code, region_code, merchant_id) = merchants[merchant_index];
            match field_name {
                "merchant_name" => name.to_string(),
                "merchant_dba" => dba.to_string(),
                "merchant_legal_name" => legal_name.to_string(),
                "merchant_category_code" | "merchant_code" => code.to_string(),
                "business_region_code" => region_code.to_string(),
                "merchant_id" => merchant_id.to_string(),
                _ => "".to_string()
            }
        },
        "merchant_risk_indicator" => generate_from_options_with_rng(&["LOW", "MEDIUM", "HIGH"], &mut seeded_rng),
        "payment_acc_indicator" => generate_from_options_with_rng(&["01", "02", "03", "04"], &mut seeded_rng),
        "payment_method" | "wallet_type" => {
            let payment_method = generate_from_options_with_rng(&["CARD", "BANK", "WALLET", "CRYPTO"], &mut seeded_rng);
            match field_name {
                "payment_method" => payment_method,
                "wallet_type" => {
                    if payment_method == "WALLET" {
                        generate_from_options_with_rng(&["APPLE_PAY", "GOOGLE_PAY", "SAMSUNG_PAY", "PAYPAL"], &mut seeded_rng)
                    } else {
                        "N/A".to_string()
                    }
                },
                _ => unreachable!()
            }
        },
        "pci_pattern" => generate_from_options_with_rng(&["DSS", "P2PE", "TSP"], &mut seeded_rng),
        "pos_condition_code" => generate_from_options_with_rng(&["00", "01", "02", "03"], &mut seeded_rng),
        "pos_entry_mode" => generate_from_options_with_rng(&["01", "02", "05", "90"], &mut seeded_rng),
        "risk_analysis_result" => generate_from_options_with_rng(&["LOW", "MEDIUM", "HIGH"], &mut seeded_rng),
        "risk_score_tier" => generate_from_options_with_rng(&["LOW", "MEDIUM", "HIGH", "CRITICAL"], &mut seeded_rng),
        "sdk_info" => generate_from_options_with_rng(&["iOS_SDK_1.0", "Android_SDK_1.0", "Web_SDK_1.0"], &mut seeded_rng),
        "session_id" => generate_prefixed_id_with_rng("SESS", 16, &mut seeded_rng),
        "ship_addr_city" | "ship_addr_country" | "ship_addr_line1" | "ship_addr_line2" | "ship_addr_line3" | "ship_addr_post_code" | "ship_addr_state" => {
            let shipping_country_options = [
                "USA", "CAN", "GBR", "DEU", "FRA", "AUS", "JPN", "ITA", "ESP", "NLD", "BEL", "CHE", 
                "AUT", "SWE", "NOR", "DNK", "FIN", "IRL", "PRT", "GRC", "POL", "CZE", "HUN", "SVK",
                "SVN", "EST", "LVA", "LTU", "LUX", "MLT", "CYP", "BGR", "ROU", "HRV", "MEX", "BRA",
                "ARG", "CHL", "COL", "PER", "VEN", "URY", "PRY", "BOL", "ECU", "GUY", "SUR", "GUF"
            ];
            let country = shipping_country_options[seeded_rng.gen_range(0..shipping_country_options.len())];
            
            match field_name {
                "ship_addr_country" => country.to_string(),
                "ship_addr_city" => {
                    match country {
                        "USA" => ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][seeded_rng.gen_range(0..5)],
                        "CAN" => ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"][seeded_rng.gen_range(0..5)],
                        "GBR" => ["London", "Manchester", "Birmingham", "Liverpool", "Leeds"][seeded_rng.gen_range(0..5)],
                        "DEU" => ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"][seeded_rng.gen_range(0..5)],
                        "FRA" => ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"][seeded_rng.gen_range(0..5)],
                        "AUS" => ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"][seeded_rng.gen_range(0..5)],
                        "JPN" => ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"][seeded_rng.gen_range(0..5)],
                        "ITA" => ["Rome", "Milan", "Naples", "Turin", "Florence"][seeded_rng.gen_range(0..5)],
                        "ESP" => ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"][seeded_rng.gen_range(0..5)],
                        "NLD" => ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"][seeded_rng.gen_range(0..5)],
                        "BEL" => ["Brussels", "Antwerp", "Ghent", "Charleroi", "Liege"][seeded_rng.gen_range(0..5)],
                        "CHE" => ["Zurich", "Geneva", "Basel", "Bern", "Lausanne"][seeded_rng.gen_range(0..5)],
                        "AUT" => ["Vienna", "Salzburg", "Innsbruck", "Graz", "Linz"][seeded_rng.gen_range(0..5)],
                        "SWE" => ["Stockholm", "Gothenburg", "Malmo", "Uppsala", "Vasteras"][seeded_rng.gen_range(0..5)],
                        "NOR" => ["Oslo", "Bergen", "Trondheim", "Stavanger", "Drammen"][seeded_rng.gen_range(0..5)],
                        "DNK" => ["Copenhagen", "Aarhus", "Odense", "Aalborg", "Esbjerg"][seeded_rng.gen_range(0..5)],
                        "FIN" => ["Helsinki", "Espoo", "Tampere", "Vantaa", "Turku"][seeded_rng.gen_range(0..5)],
                        "IRL" => ["Dublin", "Cork", "Limerick", "Galway", "Waterford"][seeded_rng.gen_range(0..5)],
                        "PRT" => ["Lisbon", "Porto", "Vila Nova de Gaia", "Amadora", "Braga"][seeded_rng.gen_range(0..5)],
                        "GRC" => ["Athens", "Thessaloniki", "Patras", "Heraklion", "Larissa"][seeded_rng.gen_range(0..5)],
                        "POL" => ["Warsaw", "Krakow", "Lodz", "Wroclaw", "Poznan"][seeded_rng.gen_range(0..5)],
                        "CZE" => ["Prague", "Brno", "Ostrava", "Plzen", "Liberec"][seeded_rng.gen_range(0..5)],
                        "HUN" => ["Budapest", "Debrecen", "Szeged", "Miskolc", "Pecs"][seeded_rng.gen_range(0..5)],
                        "SVK" => ["Bratislava", "Kosice", "Presov", "Zilina", "Banska Bystrica"][seeded_rng.gen_range(0..5)],
                        "SVN" => ["Ljubljana", "Maribor", "Celje", "Kranj", "Velenje"][seeded_rng.gen_range(0..5)],
                        "EST" => ["Tallinn", "Tartu", "Narva", "Parnu", "Kohtla-Jarve"][seeded_rng.gen_range(0..5)],
                        "LVA" => ["Riga", "Daugavpils", "Liepaja", "Jelgava", "Jurmala"][seeded_rng.gen_range(0..5)],
                        "LTU" => ["Vilnius", "Kaunas", "Klaipeda", "Siauliai", "Panevezys"][seeded_rng.gen_range(0..5)],
                        "LUX" => ["Luxembourg City", "Esch-sur-Alzette", "Differdange", "Dudelange", "Ettelbruck"][seeded_rng.gen_range(0..5)],
                        "MLT" => ["Valletta", "Birkirkara", "Mosta", "Qormi", "Zabbar"][seeded_rng.gen_range(0..5)],
                        "CYP" => ["Nicosia", "Limassol", "Larnaca", "Famagusta", "Paphos"][seeded_rng.gen_range(0..5)],
                        "BGR" => ["Sofia", "Plovdiv", "Varna", "Burgas", "Ruse"][seeded_rng.gen_range(0..5)],
                        "ROU" => ["Bucharest", "Cluj-Napoca", "Timisoara", "Iasi", "Constanta"][seeded_rng.gen_range(0..5)],
                        "HRV" => ["Zagreb", "Split", "Rijeka", "Osijek", "Zadar"][seeded_rng.gen_range(0..5)],
                        "MEX" => ["Mexico City", "Guadalajara", "Monterrey", "Puebla", "Tijuana"][seeded_rng.gen_range(0..5)],
                        "BRA" => ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"][seeded_rng.gen_range(0..5)],
                        "ARG" => ["Buenos Aires", "Cordoba", "Rosario", "Mendoza", "La Plata"][seeded_rng.gen_range(0..5)],
                        "CHL" => ["Santiago", "Valparaiso", "Concepcion", "La Serena", "Antofagasta"][seeded_rng.gen_range(0..5)],
                        "COL" => ["Bogota", "Medellin", "Cali", "Barranquilla", "Cartagena"][seeded_rng.gen_range(0..5)],
                        "PER" => ["Lima", "Arequipa", "Trujillo", "Chiclayo", "Huancayo"][seeded_rng.gen_range(0..5)],
                        "VEN" => ["Caracas", "Maracaibo", "Valencia", "Barquisimeto", "Maracay"][seeded_rng.gen_range(0..5)],
                        "URY" => ["Montevideo", "Salto", "Paysandu", "Las Piedras", "Rivera"][seeded_rng.gen_range(0..5)],
                        "PRY" => ["Asuncion", "Ciudad del Este", "San Lorenzo", "Luque", "Capiata"][seeded_rng.gen_range(0..5)],
                        "BOL" => ["La Paz", "Santa Cruz", "Cochabamba", "Sucre", "Oruro"][seeded_rng.gen_range(0..5)],
                        "ECU" => ["Quito", "Guayaquil", "Cuenca", "Santo Domingo", "Machala"][seeded_rng.gen_range(0..5)],
                        "GUY" => ["Georgetown", "Linden", "New Amsterdam", "Anna Regina", "Bartica"][seeded_rng.gen_range(0..5)],
                        "SUR" => ["Paramaribo", "Lelydorp", "Brokopondo", "Nieuw Nickerie", "Moengo"][seeded_rng.gen_range(0..5)],
                        "GUF" => ["Cayenne", "Saint-Laurent-du-Maroni", "Kourou", "Remire-Montjoly", "Matoury"][seeded_rng.gen_range(0..5)],
                        _ => ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"][seeded_rng.gen_range(0..5)]
                    }.to_string()
                },
                "ship_addr_line1" => format!("{} Shipping St", seeded_rng.gen_range(100..9999)),
                "ship_addr_line2" => {
                    if seeded_rng.gen::<f32>() < 0.3 {
                        format!("Unit {}", seeded_rng.gen_range(1..999))
                    } else {
                        "".to_string()
                    }
                },
                "ship_addr_line3" => "".to_string(),
                "ship_addr_post_code" => {
                    match country {
                        "USA" => format!("{:05}", seeded_rng.gen_range(10000..99999)),
                        "CAN" => format!("{}{}{} {}{}{}", 
                            ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'X', 'Y'][seeded_rng.gen_range(0..18)] as char,
                            seeded_rng.gen_range(0..10),
                            ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..20)] as char,
                            seeded_rng.gen_range(0..10),
                            ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..20)] as char,
                            seeded_rng.gen_range(0..10)
                        ),
                        "GBR" => format!("{}{} {}{}{}", 
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..25)] as char,
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..25)] as char,
                            seeded_rng.gen_range(0..10),
                            seeded_rng.gen_range(0..10),
                            ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'][seeded_rng.gen_range(0..25)] as char
                        ),
                        _ => format!("{:05}", seeded_rng.gen_range(10000..99999))
                    }
                },
                "ship_addr_state" => {
                    match country {
                        "USA" => ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"][seeded_rng.gen_range(0..10)],
                        "CAN" => ["ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE"][seeded_rng.gen_range(0..10)],
                        "AUS" => ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"][seeded_rng.gen_range(0..8)],
                        "MEX" => ["CDMX", "JAL", "NL", "PUE", "BC", "VER", "GTO", "MICH", "CHIH", "OAX"][seeded_rng.gen_range(0..10)],
                        "BRA" => ["SP", "RJ", "MG", "BA", "PR", "RS", "PE", "CE", "PA", "SC"][seeded_rng.gen_range(0..10)],
                        "ARG" => ["BA", "CABA", "COR", "SF", "MEN", "TUC", "ENT", "CHA", "COR", "MIS"][seeded_rng.gen_range(0..10)],
                        _ => "N/A"
                    }.to_string()
                },
                _ => unreachable!()
            }
        },
        "spending_pattern" => generate_from_options_with_rng(&["NORMAL", "HIGH", "BURST"], &mut seeded_rng),
        "step_up_indicator" => generate_from_options_with_rng(&["Y", "N"], &mut seeded_rng),
        "suspicious_acc_activity" => generate_from_options_with_rng(&["Y", "N"], &mut seeded_rng),
        "terminal_id" => generate_prefixed_id_with_rng("TERM", 8, &mut seeded_rng),
        "terminal_type" => generate_from_options_with_rng(&["POS", "ATM", "MOTO", "ECOMMERCE"], &mut seeded_rng),
        "three_ds_version" => generate_from_options_with_rng(&["2.1.0", "2.2.0", "2.3.1"], &mut seeded_rng),
        "token_requestor_id" => generate_prefixed_id_with_rng("TR", 11, &mut seeded_rng),
        "tokenization_method" => generate_from_options_with_rng(&["DPAN", "NETWORK", "ISSUER"], &mut seeded_rng),
        "transaction_source" => generate_from_options_with_rng(&["ONLINE", "POS", "ATM", "MOBILE"], &mut seeded_rng),
        "transaction_timestamp" => generate_timestamp_with_rng(process_date, &mut seeded_rng).to_string(),
        "trusted_beneficiary" => generate_from_options_with_rng(&["Y", "N"], &mut seeded_rng),
        "user_agent" => generate_from_options_with_rng(&[
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.76",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
        ], &mut seeded_rng),
        "velocity_check_result" => generate_from_options_with_rng(&["PASS", "FAIL", "WARNING"], &mut seeded_rng),
        "whitelist_status" => generate_from_options_with_rng(&["WHITELISTED", "NOT_WHITELISTED", "PENDING"], &mut seeded_rng),
        
        // Specific date field handling
        "process_date" => generate_timestamp_with_rng(process_date, &mut seeded_rng).to_string(),
        "insert_date" => generate_insert_timestamp().to_string(),
        
        // Generic timestamp fields (excluding process_date and insert_date)
        name if name.contains("timestamp") || 
                (name.contains("_date") && name != "process_date" && name != "insert_date") || 
                name.contains("_time") => {
            generate_timestamp_with_rng(process_date, &mut seeded_rng).to_string()
        },
        
        "transaction_id" | "original_transaction_id" => format!("TXN{:016}", seeded_rng.gen::<u64>()),
        "clearing_id" => format!("CLR{:016}", non_seeded_rng.gen::<u64>()),
        "settlement_id" => format!("STL{:016}", non_seeded_rng.gen::<u64>()),
        
        "chargeback_status" => {
            let chargeback_check = (row_seed % 10000) as f32 / 10000.0;
            if chargeback_check < 0.005 { // 0.5% chance (matches chargeback_amount logic)
                ["INITIATED", "PENDING", "RESOLVED"][seeded_rng.gen_range(0..3)].to_string()
            } else {
                "NONE".to_string()
            }
        },
        "reason_code" | "chargeback_reason_code" => {
            if is_chargeback_table {
                // Always populate reason_code in chargeback table
                let network_brand = std::env::var("NETWORK_BRAND").unwrap_or_else(|_| "MASTERCARD".to_string());
                match network_brand.as_str() {
                    "MASTERCARD" => generate_from_options_with_rng(&["4855", "4834", "4837", "4863", "4871"], &mut seeded_rng),
                    "VISA" => generate_from_options_with_rng(&["10.4", "11.1", "12.1", "13.1", "13.2"], &mut seeded_rng),
                    "AMEX" | "AMERICAN_EXPRESS" => generate_from_options_with_rng(&["C02", "C08", "C14", "C18", "C28"], &mut seeded_rng),
                    "DISCOVER" => generate_from_options_with_rng(&["4554", "4553", "4552", "4550", "4541"], &mut seeded_rng),
                    "JCB" => generate_from_options_with_rng(&["J40", "J41", "J42", "J43", "J44"], &mut seeded_rng),
                    "DINERS" | "DINERS_CLUB" => generate_from_options_with_rng(&["D10", "D11", "D12", "D13", "D14"], &mut seeded_rng),
                    "UNIONPAY" => generate_from_options_with_rng(&["UP01", "UP02", "UP03", "UP04", "UP05"], &mut seeded_rng),
                    _ => generate_from_options_with_rng(&["4855", "4834", "4837", "4863", "4871"], &mut seeded_rng)
                }
            } else {
                // Only populate if this row_seed is selected for chargeback table
                if chargeback_row_seeds.contains(&row_seed) {
                    let network_brand = std::env::var("NETWORK_BRAND").unwrap_or_else(|_| "MASTERCARD".to_string());
                    match network_brand.as_str() {
                        "MASTERCARD" => generate_from_options_with_rng(&["4855", "4834", "4837", "4863", "4871"], &mut seeded_rng),
                        "VISA" => generate_from_options_with_rng(&["10.4", "11.1", "12.1", "13.1", "13.2"], &mut seeded_rng),
                        "AMEX" | "AMERICAN_EXPRESS" => generate_from_options_with_rng(&["C02", "C08", "C14", "C18", "C28"], &mut seeded_rng),
                        "DISCOVER" => generate_from_options_with_rng(&["4554", "4553", "4552", "4550", "4541"], &mut seeded_rng),
                        "JCB" => generate_from_options_with_rng(&["J40", "J41", "J42", "J43", "J44"], &mut seeded_rng),
                        "DINERS" | "DINERS_CLUB" => generate_from_options_with_rng(&["D10", "D11", "D12", "D13", "D14"], &mut seeded_rng),
                        "UNIONPAY" => generate_from_options_with_rng(&["UP01", "UP02", "UP03", "UP04", "UP05"], &mut seeded_rng),
                        _ => generate_from_options_with_rng(&["4855", "4834", "4837", "4863", "4871"], &mut seeded_rng)
                    }
                } else {
                    "".to_string()
                }
            }
        },
        "refund_status" => if business_logic.has_refund { ["PROCESSED", "PENDING", "FAILED"][seeded_rng.gen_range(0..3)].to_string() } else { "NONE".to_string() },
        "void_status" => if business_logic.has_void { ["VOIDED", "PENDING"][seeded_rng.gen_range(0..2)].to_string() } else { "NONE".to_string() },
        "adjustment_status" => if business_logic.has_adjustment { ["ADJUSTED", "PENDING"][seeded_rng.gen_range(0..2)].to_string() } else { "NONE".to_string() },
        
        // Network and technical data
        "sca_result" => ["AUTHENTICATED", "NOT_AUTHENTICATED", "ATTEMPTED", "UNAVAILABLE"][non_seeded_rng.gen_range(0..4)].to_string(),
        
        // Purchase and merchant data
        "clearing_batch_id" => generate_prefixed_id_with_rng("CLR_BATCH", 12, &mut seeded_rng),
        "processor_id" => generate_prefixed_id_with_rng("PROC", 8, &mut seeded_rng),
        "network_id" => generate_prefixed_id_with_rng("NET", 6, &mut seeded_rng),
        "clearing_response_code" => {
            if business_logic.is_auth_declined {
                // Declined: mostly error codes
                ["01", "02", "03", "04", "05"][seeded_rng.gen_range(0..5)].to_string()
            } else {
                // Approved: mostly 00 (success)
                if seeded_rng.gen::<f32>() < 0.9 { "00" } else { ["01", "02"][seeded_rng.gen_range(0..2)] }.to_string()
            }
        },
        "clearing_response_message" => {
            if business_logic.is_auth_declined {
                // Declined: 60% DECLINED, 25% ERROR, 15% PENDING
                let rand_val = seeded_rng.gen::<f32>();
                if rand_val < 0.6 { "DECLINED" } 
                else if rand_val < 0.85 { "ERROR" } 
                else { "PENDING" }
            } else {
                // Approved: 90% APPROVED, 10% PENDING
                if seeded_rng.gen::<f32>() < 0.9 { "APPROVED" } else { "PENDING" }
            }.to_string()
        },
        "reconciliation_status" => {
            if business_logic.is_auth_declined {
                "EXCEPTION".to_string()
            } else if chargeback_row_seeds.contains(&row_seed) {
                "UNMATCHED".to_string()
            } else {
                match business_logic.transaction_type.as_str() {
                    "REFUND" => if seeded_rng.gen::<f32>() < 0.95 { "MATCHED" } else { "PENDING" },
                    _ => if seeded_rng.gen::<f32>() < 0.88 { "MATCHED" } else { "PENDING" }
                }.to_string()
            }
        },
        "dispute_status" => {
            if chargeback_row_seeds.contains(&row_seed) {
                ["INITIATED", "PENDING", "RESOLVED"][seeded_rng.gen_range(0..3)].to_string()
            } else if business_logic.is_auth_declined {
                "NONE".to_string()
            } else {
                if seeded_rng.gen::<f32>() < 0.95 { "NONE".to_string() } else { "CLOSED".to_string() }
            }
        },
        "interchange_program" => generate_from_options_with_rng(&["STANDARD", "ENHANCED", "PREMIUM", "CORPORATE"], &mut seeded_rng),
        "interchange_rate_type" => generate_from_options_with_rng(&["QUALIFIED", "MID_QUALIFIED", "NON_QUALIFIED"], &mut seeded_rng),
        "auth_response_code" => {
            if business_logic.is_auth_declined {
                [
                    "51", "61", "05", "79", "82", "83", "04", "14", "33", "41", "43", "54", 
                    "01", "03", "13", "15", "17", "38", "57", "58", "62", "64", "65", "75", 
                    "81", "R0", "R1", "R3", "34", "36", "37", "55", "63", "66", "67", "70"
                ][seeded_rng.gen_range(0..36)].to_string()
            } else {
                match business_logic.transaction_type.as_str() {
                    "CASH_ADVANCE" => ["00", "85"][seeded_rng.gen_range(0..2)].to_string(),
                    "BALANCE_TRANSFER" => ["00", "87"][seeded_rng.gen_range(0..2)].to_string(),
                    _ => ["00", "08", "10"][seeded_rng.gen_range(0..3)].to_string()
                }
            }
        },
        "settlement_status" => {
            if business_logic.is_auth_declined {
                if seeded_rng.gen::<f32>() < 0.7 { "FAILED" } else { "CANCELLED" }
            } else {
                match business_logic.transaction_type.as_str() {
                    "REFUND" => if seeded_rng.gen::<f32>() < 0.9 { "SETTLED" } else { "PENDING" },
                    "CASH_ADVANCE" => if seeded_rng.gen::<f32>() < 0.95 { "SETTLED" } else { "PENDING" },
                    _ => if seeded_rng.gen::<f32>() < 0.85 { "SETTLED" } else { "PENDING" }
                }
            }.to_string()
        },
        "auth_response_message" => {
            if business_logic.is_auth_declined {
                match business_logic.transaction_type.as_str() {
                    "CASH_ADVANCE" => "CASH_ADVANCE_DECLINED".to_string(),
                    "BALANCE_TRANSFER" => "BALANCE_TRANSFER_DECLINED".to_string(),
                    _ => "DECLINED".to_string()
                }
            } else {
                "APPROVED".to_string()
            }
        },
        "mti" => {
            // MTI logically tied to auth response and transaction flow
            if business_logic.is_reversal && !business_logic.is_auth_declined {
                // Reversals only happen for approved transactions
                if seeded_rng.gen_bool(0.8) {
                    "0420".to_string() // Acquirer Reversal Advice (most common)
                } else {
                    "0400".to_string() // Acquirer Reversal Request
                }
            } else if business_logic.has_void {
                // Void transactions use specific MTI patterns
                "0100".to_string() // Authorization Request (for void processing)
            } else {
                // Normal transaction flow - weighted by realistic message distribution
                let flow_selector = seeded_rng.gen_range(0..100);
                match flow_selector {
                    0..=35 => "0100".to_string(), // 35% Authorization Request
                    36..=55 => "0110".to_string(), // 20% Authorization Response
                    56..=70 => "0200".to_string(), // 15% Financial Request (settlement/clearing)
                    71..=85 => "0210".to_string(), // 15% Financial Response
                    86..=90 => "0120".to_string(), // 5% Authorization Advice (timeout scenarios)
                    91..=93 => "0130".to_string(), // 3% Authorization Advice Response
                    94..=96 => "0121".to_string(), // 3% Authorization Advice Repeat
                    97..=98 => "0800".to_string(), // 2% Network Management Request
                    _ => "0810".to_string(), // 2% Network Management Response
                }
            }
        },
        "decline_reason" => {
            if business_logic.is_auth_declined {
                match business_logic.transaction_type.as_str() {
                    "CASH_ADVANCE" => ["CASH_ADVANCE_NOT_ALLOWED", "EXCEEDS_CASH_LIMIT", "INSUFFICIENT_FUNDS"][seeded_rng.gen_range(0..3)].to_string(),
                    "BALANCE_TRANSFER" => ["BALANCE_TRANSFER_NOT_ALLOWED", "EXCEEDS_CREDIT_LIMIT"][seeded_rng.gen_range(0..2)].to_string(),
                    _ => [
                        "INSUFFICIENT_FUNDS", "EXPIRED_CARD", "INVALID_CVV", "SUSPECTED_FRAUD",
                        "CARD_BLOCKED", "EXCEEDS_LIMIT", "INVALID_PIN", "CARD_NOT_ACTIVATED"
                    ][seeded_rng.gen_range(0..8)].to_string()
                }
            } else {
                "".to_string()
            }
        },
        "clearing_status" => {
            if chargeback_row_seeds.contains(&row_seed) {
                "CLEARED".to_string()
            } else if business_logic.is_auth_declined {
                "REJECTED".to_string()
            } else {
                match business_logic.transaction_type.as_str() {
                    "REFUND" => if seeded_rng.gen::<f32>() < 0.98 { "CLEARED" } else { "PENDING" },
                    "CASH_ADVANCE" => if seeded_rng.gen::<f32>() < 0.92 { "CLEARED" } else { "PENDING" },
                    _ => {
                        let rand_val = seeded_rng.gen::<f32>();
                        if rand_val < 0.95 { "CLEARED" } else if rand_val < 0.98 { "PENDING" } else { "FAILED" }
                    }
                }.to_string()
            }
        },
        "cvv_result" => generate_from_options_with_rng(&["M", "N", "P", "S", "U", "X", "Y"], &mut seeded_rng),
        "avs_result" => generate_from_options_with_rng(&["Y", "N", "A", "Z", "W", "X", "U", "R"], &mut seeded_rng),
        "processor_name" => generate_from_options_with_rng(&["FIRST_DATA", "CHASE_PAYMENTECH", "WORLDPAY", "TSYS", "ELAVON"], &mut seeded_rng),
        "settlement_batch_id" => generate_prefixed_id_with_rng("STL_BATCH", 12, &mut seeded_rng),
        "exemption_type" => generate_from_options_with_rng(&["NONE", "LOW_VALUE", "TRA", "CORPORATE", "SECURE_CORPORATE"], &mut seeded_rng),

        // Default placeholder for non-covered fields
        _ => format!("{}_{:010x}", field_name.split('_').next().unwrap_or("data"), non_seeded_rng.gen_range(0..0x10000000000u64))
    }
}

#[derive(Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub total_columns: usize,
    pub fields: SchemaFields,
}

#[derive(Serialize, Deserialize)]
pub struct SchemaFields {
    pub strings: Vec<String>,
    #[serde(default)]
    pub ints: Vec<String>,
    #[serde(default)]
    pub bigints: Vec<String>,
    #[serde(default)]
    pub decimals: Vec<String>,
    #[serde(default)]
    pub decimals_8_0: Vec<String>,
    #[serde(default)]
    pub decimals_12_0: Vec<String>,
    #[serde(default)]
    pub decimals_12_2: Vec<String>,
    #[serde(default)]
    pub decimals_13_6: Vec<String>,
    #[serde(default)]
    pub decimals_14_6: Vec<String>,
    #[serde(default)]
    pub decimals_15_6: Vec<String>,
    #[serde(default)]
    pub decimals_15_7: Vec<String>,
    #[serde(default)]
    pub decimals_18_2: Vec<String>,
    #[serde(default)]
    pub decimals_18_6: Vec<String>,
    #[serde(default)]
    pub decimals_19_0: Vec<String>,
    #[serde(default)]
    pub decimals_21_3: Vec<String>,
    #[serde(default)]
    pub decimals_23_3: Vec<String>,
    #[serde(default)]
    pub decimals_38_0: Vec<String>,
    #[serde(default)]
    pub decimals_38_2: Vec<String>,
    #[serde(default)]
    pub timestamps: Vec<String>,
    #[serde(default)]
    pub smallints: Vec<String>,
    #[serde(default)]
    pub tinyints: Vec<String>,
}

fn generate_batch_with_seeds(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
    chargeback_row_seeds: &[u64], 
    hash_pan_pool: &[String], 
    process_date: &str, 
    schema_content: &str
) -> RecordBatch {
    
    let schema: TableSchema = serde_json::from_str(schema_content).expect("Failed to load schema");
    let is_chargeback_table = chargeback_row_seeds.is_empty();
    
    let mut fields = Vec::new();
    for field_name in &schema.fields.strings {
        fields.push(Field::new(field_name.clone(), DataType::Utf8, true));
    }
    for field_name in &schema.fields.ints {
        fields.push(Field::new(field_name.clone(), DataType::Int32, true));
    }
    for field_name in &schema.fields.bigints {
        fields.push(Field::new(field_name.clone(), DataType::Int64, true));
    }
    for field_name in &schema.fields.smallints {
        fields.push(Field::new(field_name.clone(), DataType::Int16, true));
    }
    for field_name in &schema.fields.tinyints {
        fields.push(Field::new(field_name.clone(), DataType::Int8, true));
    }
    for field_name in &schema.fields.timestamps {
        fields.push(Field::new(field_name.clone(), DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true));
    }
    
    // Add decimal fields to schema
    let schema_value: serde_json::Value = serde_json::from_str(schema_content).unwrap();
    if let Some(fields_obj) = schema_value.get("fields").and_then(|f| f.as_object()) {
        for (field_type, field_list) in fields_obj {
            if field_type.starts_with("decimals_") {
                let parts: Vec<&str> = field_type.strip_prefix("decimals_").unwrap().split('_').collect();
                if parts.len() == 2 {
                    let precision: u8 = parts[0].parse().unwrap_or(18);
                    let scale: i8 = parts[1].parse().unwrap_or(2);
                    
                    if let Some(field_names) = field_list.as_array() {
                        for field_name in field_names {
                            if let Some(name) = field_name.as_str() {
                                fields.push(Field::new(name.to_string(), DataType::Decimal128(precision, scale), true));
                            }
                        }
                    }
                }
            }
        }
    }
    
    let arrow_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    
    // Generate arrays for num_rows using row_seeds for randomization
    for field_name in &schema.fields.strings {
        let values: Vec<String> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            )
        }).collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }
    
    for field_name in &schema.fields.ints {
        let values: Vec<i32> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
            let field_value = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool,
            );
            field_value.parse::<i32>().unwrap_or_else(|_| rng.gen_range(1..1000000))
        }).collect();
        arrays.push(Arc::new(Int32Array::from(values)));
    }

    for field_name in &schema.fields.bigints {
        let values: Vec<i64> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
            let field_value = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            field_value.parse::<i64>().unwrap_or_else(|_| rng.gen_range(1..1000000000))
        }).collect();
        arrays.push(Arc::new(Int64Array::from(values)));
    }

    for field_name in &schema.fields.smallints {
        let values: Vec<i16> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
            let field_value = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            field_value.parse::<i16>().unwrap_or_else(|_| rng.gen_range(1..30000))
        }).collect();
        arrays.push(Arc::new(Int16Array::from(values)));
    }

    for field_name in &schema.fields.tinyints {
        let values: Vec<i8> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
            let field_value = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            field_value.parse::<i8>().unwrap_or_else(|_| rng.gen_range(1..100))
        }).collect();
        arrays.push(Arc::new(Int8Array::from(values)));
    }

    for field_name in &schema.fields.timestamps {
        let values: Vec<i64> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let timestamp_str = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            timestamp_str.parse::<i64>().unwrap_or_else(|_| chrono::Utc::now().timestamp_micros())
        }).collect();
        arrays.push(Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")));
    }
    
    // Handle decimal arrays
    if let Some(fields_obj) = schema_value.get("fields").and_then(|f| f.as_object()) {
        for (field_type, field_list) in fields_obj {
            if field_type.starts_with("decimals_") {
                let parts: Vec<&str> = field_type.strip_prefix("decimals_").unwrap().split('_').collect();
                if parts.len() == 2 {
                    let precision: u8 = parts[0].parse().unwrap_or(18);
                    let scale: i8 = parts[1].parse().unwrap_or(2);
                    
                    if let Some(field_names) = field_list.as_array() {
                        for field_name in field_names {
                            if let Some(name) = field_name.as_str() {
                                let values: Vec<i128> = (0..num_rows).map(|row_index| {
                                    let row_seed = row_seeds[row_index % row_seeds.len()];
                                    let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
                                    let field_value = generate_data_for_given_field(
                                        name, 
                                        row_seed, 
                                        row_index as i64,
                                        partition_job_order,
                                        thread_id,     
                                        num_threads,
                                        chargeback_row_seeds, 
                                        is_chargeback_table, 
                                        process_date, 
                                        hash_pan_pool
                                    );
                                    field_value.parse::<f64>().unwrap_or_else(|_| rng.gen_range(1.0..1000000.0)) as i128 * 10_i128.pow(scale as u32)
                                }).collect();
                                arrays.push(Arc::new(Decimal128Array::from(values).with_precision_and_scale(precision, scale).unwrap()));
                            }
                        }
                    }
                }
            }
        }
    }
    
    RecordBatch::try_new(arrow_schema, arrays).unwrap()
}

// Unified hash batch generator using row seeds
fn generate_hash_batch_with_seeds(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
    chargeback_row_seeds: &[u64], 
    hash_pan_pool: &[String], 
    process_date: &str, 
    schema_content: &str
) -> RecordBatch {

    let schema: TableSchema = serde_json::from_str(schema_content).expect("Failed to load hash schema");
    let is_chargeback_table = chargeback_row_seeds.is_empty();
    
    let mut fields = Vec::new();
    for field_name in &schema.fields.strings {
        fields.push(Field::new(field_name.clone(), DataType::Utf8, true));
    }
    for field_name in &schema.fields.ints {
        fields.push(Field::new(field_name.clone(), DataType::Int32, true));
    }
    for field_name in &schema.fields.bigints {
        fields.push(Field::new(field_name.clone(), DataType::Int64, true));
    }
    for field_name in &schema.fields.timestamps {
        fields.push(Field::new(field_name.clone(), DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true));
    }
    
    let arrow_schema = Arc::new(Schema::new(fields));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    
    // Generate arrays for num_rows using row_seeds for randomization
    for field_name in &schema.fields.strings {
        let values: Vec<String> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            )
        }).collect();
        arrays.push(Arc::new(StringArray::from(values)));
    }
    
    for field_name in &schema.fields.ints {
        let values: Vec<i32> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
            let field_value = generate_data_for_given_field(
                field_name, 
                row_seed,
                row_index as i64, 
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            field_value.parse::<i32>().unwrap_or_else(|_| rng.gen_range(1..1000000))
        }).collect();
        arrays.push(Arc::new(Int32Array::from(values)));
    }
    
    for field_name in &schema.fields.bigints {
        let values: Vec<i64> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let mut rng = rand::rngs::StdRng::seed_from_u64(row_seed);
            let field_value = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            field_value.parse::<i64>().unwrap_or_else(|_| rng.gen_range(1..1000000000))
        }).collect();
        arrays.push(Arc::new(Int64Array::from(values)));
    }

    for field_name in &schema.fields.timestamps {
        let values: Vec<i64> = (0..num_rows).map(|row_index| {
            let row_seed = row_seeds[row_index % row_seeds.len()];
            let timestamp_str = generate_data_for_given_field(
                field_name, 
                row_seed, 
                row_index as i64,
                partition_job_order,
                thread_id,     
                num_threads,
                chargeback_row_seeds, 
                is_chargeback_table, 
                process_date, 
                hash_pan_pool
            );
            timestamp_str.parse::<i64>().unwrap_or_else(|_| chrono::Utc::now().timestamp_micros())
        }).collect();
        arrays.push(Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")));
    }
    
    RecordBatch::try_new(arrow_schema, arrays).unwrap()
}

pub fn generate_authorization_batch(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
    chargeback_row_seeds: &[u64], 
    hash_pan_pool: &[String], 
    process_date: &str
) -> RecordBatch {

    let schema_content = include_str!("../schemas/authorization_schema.json");
    generate_batch_with_seeds(
        num_rows, 
        row_seeds, 
        partition_job_order, 
        thread_id, 
        num_threads, 
        chargeback_row_seeds, 
        hash_pan_pool, 
        process_date, 
        schema_content
    )
}

pub fn generate_clearing_batch(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
    chargeback_row_seeds: &[u64], 
    hash_pan_pool: &[String], 
    process_date: &str
) -> RecordBatch {

    let schema_content = include_str!("../schemas/clearing_schema.json");
    generate_batch_with_seeds(
        num_rows, 
        row_seeds, 
        partition_job_order, 
        thread_id, 
        num_threads, 
        chargeback_row_seeds, 
        hash_pan_pool, 
        process_date, 
        schema_content
    )
}

pub fn generate_chargeback_batch(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
    hash_pan_pool: &[String], 
    process_date: &str
) -> RecordBatch {

    let schema_content = include_str!("../schemas/chargeback_schema.json");
    generate_batch_with_seeds(
        num_rows, 
        row_seeds, 
        partition_job_order, 
        thread_id, 
        num_threads, 
        &[], 
        hash_pan_pool, 
        process_date, 
        schema_content
    )
}

pub fn generate_authorization_hash_batch(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64, 
    thread_id: i32, 
    num_threads: i32, 
    chargeback_row_seeds: &[u64], 
    hash_pan_pool: &[String], 
    process_date: &str
) -> RecordBatch {
    
    let schema_content = include_str!("../schemas/authorization_hash_schema.json");
    generate_hash_batch_with_seeds(
        num_rows, 
        row_seeds, 
        partition_job_order, 
        thread_id, 
        num_threads, 
        chargeback_row_seeds, 
        hash_pan_pool, 
        process_date, 
        schema_content
    )
}

pub fn generate_clearing_hash_batch(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64, 
    thread_id: i32, 
    num_threads: i32, 
    chargeback_row_seeds: &[u64], 
    hash_pan_pool: &[String], 
    process_date: &str
) -> RecordBatch {

    let schema_content = include_str!("../schemas/clearing_hash_schema.json");
    generate_hash_batch_with_seeds(
        num_rows,
        row_seeds, 
        partition_job_order, 
        thread_id, 
        num_threads, 
        chargeback_row_seeds, 
        hash_pan_pool, 
        process_date, 
        schema_content
    )
}

pub fn generate_chargeback_hash_batch(
    num_rows: usize, 
    row_seeds: &[u64], 
    partition_job_order: i64, 
    thread_id: i32, 
    num_threads: i32, 
    hash_pan_pool: &[String], 
    process_date: &str
) -> RecordBatch {

    let schema_content = include_str!("../schemas/chargeback_hash_schema.json");
    generate_hash_batch_with_seeds(
        num_rows, 
        row_seeds, 
        partition_job_order, 
        thread_id, 
        num_threads, 
        &[], 
        hash_pan_pool, 
        process_date, 
        schema_content
    )
}
