use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::env;

#[derive(Debug, Clone)]
pub struct TransactionBusinessLogic {
    pub is_auth_declined: bool,
    pub is_reversal: bool,
    pub merchant_country_and_currency_map: HashMap<String, String>,
    pub issuer_country_and_currency_map: HashMap<String, String>,
    pub base_amount: f64,
    pub chargeback_multiplier: f64,

    // Transaction state (mutually exclusive)
    pub has_refund: bool,
    pub has_void: bool,
    pub has_adjustment: bool,

    // Amount features (conditional on transaction state)
    pub has_tip: bool,
    pub has_shipping: bool,

    // Transaction type
    pub transaction_type: String,
    pub has_handling: bool,
    pub has_warranty: bool,

    // Fees (conditional on issues)
    pub has_reconciliation_fee: bool,

    // Rate fields for payment processing
    pub issuer_rate: f64,
    pub network_rate: f64,
    pub risk_rate: f64,
    pub acquirer_rate: f64,
    pub exchange_rate: f64,
    pub interchange_rate: f64,
    pub processing_rate: f64,
}

pub fn generate_transaction_business_logic(
    seeded_rng: &mut rand::rngs::StdRng,
) -> TransactionBusinessLogic {
    let is_auth_declined = seeded_rng.gen::<f32>() < 0.05; // 5% decline rate
    let is_reversal = seeded_rng.gen::<f32>() < 0.005; // 0.5% reversal rate

    // Create currency maps
    let mut merchant_country_and_currency_map = HashMap::new();
    let mut issuer_country_and_currency_map = HashMap::new();

    // Populate currency mappings
    let country_currency_mappings = [
        ("USA", "USD"),
        ("CAN", "CAD"),
        ("GBR", "GBP"),
        ("JPN", "JPY"),
        ("AUS", "AUD"),
        ("CHE", "CHF"),
        ("SWE", "SEK"),
        ("NOR", "NOK"),
        ("DNK", "DKK"),
        ("POL", "PLN"),
        ("CZE", "CZK"),
        ("HUN", "HUF"),
        ("BGR", "BGN"),
        ("ROU", "RON"),
        ("KOR", "KRW"),
        ("MEX", "MXN"),
        ("BRA", "BRL"),
        ("ARG", "ARS"),
        ("CHL", "CLP"),
        ("COL", "COP"),
        ("PER", "PEN"),
        ("ARE", "AED"),
        ("ZAF", "ZAR"),
        ("SGP", "SGD"),
        // EUR countries
        ("DEU", "EUR"),
        ("FRA", "EUR"),
        ("ITA", "EUR"),
        ("ESP", "EUR"),
        ("NLD", "EUR"),
        ("BEL", "EUR"),
        ("AUT", "EUR"),
        ("IRL", "EUR"),
        ("PRT", "EUR"),
        ("GRC", "EUR"),
        ("FIN", "EUR"),
        ("SVN", "EUR"),
        ("EST", "EUR"),
        ("LVA", "EUR"),
        ("LTU", "EUR"),
        ("LUX", "EUR"),
        ("MLT", "EUR"),
        ("CYP", "EUR"),
        ("HRV", "EUR"),
    ];

    // Select specific merchant and issuer countries for this row
    let selected_merchant =
        country_currency_mappings[seeded_rng.gen_range(0..country_currency_mappings.len())];
    let selected_issuer =
        country_currency_mappings[seeded_rng.gen_range(0..country_currency_mappings.len())];

    // Store only the selected countries in the maps
    merchant_country_and_currency_map.insert(
        selected_merchant.0.to_string(),
        selected_merchant.1.to_string(),
    );
    issuer_country_and_currency_map
        .insert(selected_issuer.0.to_string(), selected_issuer.1.to_string());

    let base_amount = seeded_rng.gen_range(10.00..9999.99);
    let chargeback_multiplier = seeded_rng.gen_range(0.5..1.0);

    // Transaction state (mutually exclusive)
    let state_rand = seeded_rng.gen_range(0..100);
    let has_refund = state_rand < 5; // 5%
    let has_void = !has_refund && state_rand < 7; // 2%
    let has_adjustment = !has_refund && !has_void && state_rand < 10; // 3%

    // Amount features - only for successful, non-problematic transactions
    let has_tip = !is_auth_declined && !has_refund && seeded_rng.gen::<f32>() < 0.4;
    let has_shipping = seeded_rng.gen::<f32>() < 0.6; // Physical goods
    let has_handling = has_shipping && seeded_rng.gen::<f32>() < 0.5; // 50% of shipped items
    let has_warranty = seeded_rng.gen::<f32>() < 0.1; // 10% regardless

    // Reconciliation fee - only when there are issues or adjustments
    let has_reconciliation_fee = has_adjustment || has_void || (seeded_rng.gen::<f32>() < 0.05);

    // Realistic transaction type distribution
    let transaction_type = {
        let rand_val = seeded_rng.gen_range(0..100);
        match rand_val {
            0..=84 => "PURCHASE".to_string(),          // 85%
            85..=87 => "REFUND".to_string(),           // 3%
            88..=92 => "CASH_ADVANCE".to_string(),     // 5%
            93..=96 => "BALANCE_TRANSFER".to_string(), // 4%
            97..=98 => "FEE".to_string(),              // 2%
            _ => "ADJUSTMENT".to_string(),             // 1%
        }
    };

    // Generate payment processing rates based on transaction characteristics
    let issuer_rate = match transaction_type.as_str() {
        "CASH_ADVANCE" => seeded_rng.gen_range(0.025..0.045), // 2.5% to 4.5% for cash advances
        "BALANCE_TRANSFER" => seeded_rng.gen_range(0.015..0.035), // 1.5% to 3.5% for balance transfers
        "PURCHASE" => seeded_rng.gen_range(0.005..0.025),         // 0.5% to 2.5% for purchases
        _ => seeded_rng.gen_range(0.008..0.020), // 0.8% to 2.0% for other types
    };

    let network_rate = {
        // Network rates are typically lower and more consistent
        let base_rate = seeded_rng.gen_range(0.0001..0.0015); // 0.01% to 0.15%
                                                              // International transactions have higher network fees
        if selected_merchant.0 != selected_issuer.0 {
            base_rate * seeded_rng.gen_range(1.5..2.5) // 50% to 150% markup for cross-border
        } else {
            base_rate
        }
    };

    let risk_rate = {
        let base_rate = if is_auth_declined {
            seeded_rng.gen_range(0.008..0.015) // Higher risk rate for declined transactions
        } else {
            seeded_rng.gen_range(0.0001..0.008) // 0.01% to 0.8% for approved transactions
        };
        // Adjust based on transaction characteristics
        let multiplier = if has_tip { 1.2 } else { 1.0 };
        base_rate * multiplier
    };

    let acquirer_rate = {
        let base_rate = match transaction_type.as_str() {
            "CASH_ADVANCE" => seeded_rng.gen_range(0.025..0.050), // 2.5% to 5.0%
            "BALANCE_TRANSFER" => seeded_rng.gen_range(0.020..0.040), // 2.0% to 4.0%
            "PURCHASE" => seeded_rng.gen_range(0.015..0.035),     // 1.5% to 3.5%
            _ => seeded_rng.gen_range(0.018..0.030),              // 1.8% to 3.0%
        };
        // Adjust for merchant size (smaller merchants pay higher rates)
        if base_amount < 100.0 {
            base_rate * seeded_rng.gen_range(1.1..1.3) // 10% to 30% markup for small transactions
        } else {
            base_rate
        }
    };

    let exchange_rate = {
        if selected_merchant.1 == selected_issuer.1 {
            1.0 // Same currency = 1:1 rate
        } else {
            // Base rates to USD (as of Nov 2025)
            let to_usd_rate = match selected_issuer.1 {
                "USD" => 1.0,
                "EUR" => 1.05,
                "GBP" => 1.27,
                "JPY" => 0.0067,
                "CAD" => 0.72,
                "AUD" => 0.65,
                "CHF" => 1.13,
                "SEK" => 0.096,
                "NOK" => 0.091,
                "DKK" => 0.14,
                "PLN" => 0.25,
                "CZK" => 0.042,
                "HUF" => 0.0027,
                "BGN" => 0.54,
                "RON" => 0.21,
                "KRW" => 0.00075,
                "MXN" => 0.049,
                "BRL" => 0.17,
                "ARS" => 0.0010,
                "CLP" => 0.0010,
                "COP" => 0.00023,
                "PEN" => 0.26,
                "AED" => 0.27,
                "ZAR" => 0.055,
                "SGD" => 0.74,
                _ => 1.0,
            };
            
            let from_usd_rate = match selected_merchant.1 {
                "USD" => 1.0,
                "EUR" => 0.95,
                "GBP" => 0.79,
                "JPY" => 149.5,
                "CAD" => 1.39,
                "AUD" => 1.54,
                "CHF" => 0.88,
                "SEK" => 10.4,
                "NOK" => 11.0,
                "DKK" => 7.1,
                "PLN" => 4.0,
                "CZK" => 23.8,
                "HUF" => 370.0,
                "BGN" => 1.86,
                "RON" => 4.75,
                "KRW" => 1330.0,
                "MXN" => 20.4,
                "BRL" => 5.8,
                "ARS" => 1000.0,
                "CLP" => 970.0,
                "COP" => 4350.0,
                "PEN" => 3.85,
                "AED" => 3.67,
                "ZAR" => 18.2,
                "SGD" => 1.35,
                _ => 1.0,
            };
            
            // Calculate cross rate with ±2% variance for market fluctuation
            let base_rate = to_usd_rate * from_usd_rate;
            base_rate * seeded_rng.gen_range(0.98..1.02)
        }
    };

    let interchange_rate = {
        let base_rate = match transaction_type.as_str() {
            "CASH_ADVANCE" => seeded_rng.gen_range(0.020..0.025), // 2.0% to 2.5%
            "BALANCE_TRANSFER" => seeded_rng.gen_range(0.015..0.020), // 1.5% to 2.0%
            "PURCHASE" => seeded_rng.gen_range(0.005..0.020),     // 0.5% to 2.0%
            _ => seeded_rng.gen_range(0.008..0.015),              // 0.8% to 1.5%
        };
        // Higher for cross-border
        if selected_merchant.0 != selected_issuer.0 {
            base_rate * seeded_rng.gen_range(1.2..1.5)
        } else {
            base_rate
        }
    };

    let processing_rate = {
        // Flat processing fee, less variable
        let base_rate = seeded_rng.gen_range(0.001..0.005); // 0.1% to 0.5%
        // Slightly higher for declined transactions (more processing overhead)
        if is_auth_declined {
            base_rate * 1.2
        } else {
            base_rate
        }
    };

    TransactionBusinessLogic {
        is_auth_declined,
        is_reversal,
        merchant_country_and_currency_map,
        issuer_country_and_currency_map,
        base_amount,
        chargeback_multiplier,
        has_refund,
        has_void,
        has_adjustment,
        has_tip,
        has_shipping,
        has_handling,
        has_warranty,
        has_reconciliation_fee,
        transaction_type,
        issuer_rate,
        network_rate,
        risk_rate,
        acquirer_rate,
        exchange_rate,
        interchange_rate,
        processing_rate,
    }
}

/// Generate UTC timestamp with seeded random time component for process_date
pub fn generate_timestamp_with_rng<R: Rng>(date: &str, rng: &mut R) -> i64 {
    let naive_date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .unwrap_or_else(|_| NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());

    let hour = rng.gen_range(0..24);
    let minute = rng.gen_range(0..60);
    let second = rng.gen_range(0..60);

    let naive_time = NaiveTime::from_hms_opt(hour, minute, second).unwrap();
    let naive_datetime = NaiveDateTime::new(naive_date, naive_time);
    let utc_datetime = Utc.from_utc_datetime(&naive_datetime);

    utc_datetime.timestamp_micros()
}

/// Generate insert_date as current UTC timestamp (time of database insertion)
pub fn generate_insert_timestamp() -> i64 {
    Utc::now().timestamp_micros()
}

/// Generate alphanumeric string of specified length
pub fn generate_alphanumeric_string_with_rng<R: Rng>(length: usize, rng: &mut R) -> String {
    let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".chars().collect();
    (0..length)
        .map(|_| chars[rng.gen_range(0..chars.len())])
        .collect()
}

/// Generate ID with prefix and specified digit length using provided RNG
pub fn generate_prefixed_id_with_rng<R: Rng>(prefix: &str, digits: usize, rng: &mut R) -> String {
    if digits == 0 {
        return prefix.to_string();
    }
    let safe_digits = digits.min(18);
    let max_val = 10_u64.pow(safe_digits as u32);
    let min_val = if safe_digits == 1 {
        0
    } else {
        10_u64.pow((safe_digits - 1) as u32)
    };
    format!(
        "{}{:0width$}",
        prefix,
        rng.gen_range(min_val..max_val),
        width = safe_digits
    )
}

/// Generate 64-character SHA256-style hash using provided RNG
pub fn generate_sha256_hash_with_rng<R: Rng>(rng: &mut R) -> String {
    format!(
        "{:016x}{:016x}{:016x}{:016x}",
        rng.gen::<u64>(),
        rng.gen::<u64>(),
        rng.gen::<u64>(),
        rng.gen::<u64>()
    )
}

/// Generate value from predefined options using provided RNG
pub fn generate_from_options_with_rng<R: Rng>(options: &[&str], rng: &mut R) -> String {
    if options.is_empty() {
        return generate_generic_data_with_rng("unknown_field", rng);
    }
    options[rng.gen_range(0..options.len())].to_string()
}

/// Generate generic data for unknown fields using provided RNG
pub fn generate_generic_data_with_rng<R: Rng>(field_name: &str, rng: &mut R) -> String {
    match field_name {
        name if name.contains("_hash") => {
            format!(
                "{:016x}{:016x}{:016x}{:016x}",
                rng.gen::<u64>(),
                rng.gen::<u64>(),
                rng.gen::<u64>(),
                rng.gen::<u64>()
            )
        }
        name if name.contains("_id") => format!("ID{:012}", rng.gen::<u64>()),
        name if name.contains("address") => format!("{} Main St", rng.gen_range(100..9999)),
        name if name.contains("name") => {
            let names = [
                "Amazon Marketplace",
                "Walmart Supercenter",
                "Target Corporation",
                "Starbucks Coffee",
                "Shell Gas Station",
                "McDonald's Restaurant",
            ];
            names[rng.gen_range(0..names.len())].to_string()
        }
        name if name.contains("message") => {
            let messages = [
                "APPROVED",
                "DECLINED - INSUFFICIENT FUNDS",
                "EXPIRED CARD",
                "INVALID CVV",
                "SUSPECTED FRAUD",
            ];
            messages[rng.gen_range(0..messages.len())].to_string()
        }
        _ => {
            let prefix = field_name.split('_').next().unwrap_or("data");
            format!("{}_{:08x}", prefix, rng.gen::<u32>())
        }
    }
}

pub fn generate_partition_sequence_number(
    row_index: i64,
    partition_job_order: i64,
    thread_id: i32,
    num_threads: i32,
) -> String {
    let num_rows = env::var("NUM_OF_ROWS")
        .unwrap_or("250000".to_string())
        .parse::<i64>()
        .unwrap_or(250000);

    let job_base = partition_job_order * (num_rows * num_threads as i64);
    let thread_offset = (thread_id as i64 - 1) * num_rows;
    let sequence_number = 1000000000000001 + job_base + thread_offset + row_index;

    sequence_number.to_string()
}
