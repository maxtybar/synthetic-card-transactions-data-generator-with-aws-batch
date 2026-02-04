# AWS Batch Data Generation Workflow

This document details the complete workflow for deploying and running the 6-table batch data generation system with deterministic sequence numbering and realistic business logic.

## Overview

The system generates synthetic payment data across 6 related tables using AWS Batch with multi-threaded Rust containers. Each job runs 3 independent threads, each generating a complete set of tables with unique DynamoDB hash_pan values and sequential sequence numbers. Features realistic transaction patterns, atomic sequence coordination, and dual bucket uploads. Default configuration generates 10TB of data, configurable via `apps/job-deployment-parameters.json`.

## Architecture Components

### Infrastructure (CDK)
- **AWS Batch**: Job queue with 8,000 vCPUs (4,000 spot + 4,000 on-demand)
- **ECR Repository**: Stores Docker images with Rust data generators
- **S3 Buckets**: Combined bucket + 3 specialized buckets (clearing, authorization, chargeback)
- **VPC + Endpoints**: Private connectivity to all AWS services
- **DynamoDB Tables**: 
  - Hash PAN table: 100k Visa (by default, configurable in `job-deploy-ment-parameters.json`) PAN records with SHA256 hashes
  - Partition Counter table: Atomic counters for sequence number coordination
- **Lambda**: Seeds DynamoDB with realistic payment data

### Data Generation (Rust)
- **Multi-threaded**: 3 threads per job container
- **Sequential Numbering**: Atomic DynamoDB counters ensure perfect sequence numbering per partition
- **Business Logic**: Realistic transaction patterns with weighted distributions
- **Independent Threads**: Each gets unique DynamoDB hash_pan
- **Dual Bucket Upload**: Each thread uploads to both combined and specialized buckets

## Deployment Workflow

### 1. Infrastructure Setup
```bash
make setup && make deploy
```

**What happens:**
1. **CDK Bootstrap**: Prepares AWS environment for CDK deployment
2. **Infrastructure Deploy**: Creates Batch, ECR, S3, VPC, DynamoDB tables, Lambda
3. **DynamoDB Seeding**: Lambda generates 100k hashed payment card numbers
4. **Partition Counter Setup**: Creates atomic counter table for sequence coordination
5. **Docker Build**: CodeBuild builds Rust container with data generators
6. **ECR Push**: Uploads container image to ECR repository
7. **Job Submission**: Submits batch jobs based on TARGET_TB configuration

### 2. Sequence Number Coordination

#### Atomic Counter Flow
1. **Job Starts**: Calculates partition date using deterministic hash of job_index
2. **DynamoDB Atomic Increment**: 
   ```rust
   UPDATE partition_counter_table 
   SET job_counter = job_counter + 1 
   WHERE partition_date = "2023-05-15"
   ```
3. **Sequential Assignment**: 
   - First job for partition: gets counter=1, order=0
   - Second job for partition: gets counter=2, order=1
   - Third job for partition: gets counter=3, order=2
4. **Sequence Allocation**: Each job allocates 1,500,000 sequential numbers (3 threads × 500k rows)
   - Job 0: 1000000000000001 - 1000000001500000
   - Job 1: 1000000001500001 - 1000000003000000
   - Job 2: 1000000003000001 - 1000000004500000

### 3. Job Execution Flow

#### Per Job (352,000 total jobs)
1. **Container Start**: AWS Batch starts Fargate container (4 vCPUs, 28GB RAM)
2. **Partition Coordination**: Job gets atomic sequential order from DynamoDB
3. **Environment Setup**: Sets PARTITION_JOB_ORDER for sequence generation
4. **Thread Spawn**: Container spawns 3 independent threads
5. **Thread Execution**: Each thread runs independently:
   - Gets unique `hash_pan` from DynamoDB using thread-specific seed
   - Generates sequential sequence_numbers within job's allocation
   - Applies realistic business logic for transaction patterns
   - Makes independent chargeback decision (0.1% probability)
   - Generates complete table set with proper field relationships
   - Uploads all tables to both combined and specialized buckets in parallel
6. **Job Completion**: All 3 threads complete, container terminates

#### Thread Architecture with Sequential Numbering
```
Job Index 12345 (gets partition order 5 for "2023-05-15"):
├── Base sequence: 1000000000000001 + (5 × 1,500,000) = 1000000007500001
├── Thread 1: sequences 1000000007500001 - 1000000008000000 (500k rows)
├── Thread 2: sequences 1000000008000001 - 1000000008500000 (500k rows)
└── Thread 3: sequences 1000000008500001 - 1000000009000000 (500k rows)
```

### 4. Business Logic Implementation

#### Realistic Transaction Patterns
- **Transaction Types**: 85% PURCHASE, 5% CASH_ADVANCE, 4% BALANCE_TRANSFER, 3% REFUND, etc.
- **Card Products**: Network-accurate mapping
  - Visa: Chase Sapphire Preferred (CSP), Chase Sapphire Reserve (CSR), Chase Freedom Unlimited (CFU)
  - Mastercard: Chase Freedom Flex (CFX), World of Hyatt (WOH), Chase Business (BUS)
  - Amex: Platinum (PLT), Gold (GLD), Green (GRN)
  - Discover: Standard Discover cards

#### Field Relationships
- **MTI Codes**: Realistic message type indicators based on transaction flow
- **Auth Response Codes**: Transaction-specific codes (CASH_ADVANCE uses 00/85, BALANCE_TRANSFER uses 00/87)
- **Settlement Status**: Higher settlement rates for REFUND (90%) and CASH_ADVANCE (95%)
- **Decline Reasons**: Transaction-specific (cash advance limits, balance transfer restrictions)
- **Risk Patterns**: Amount-based alert patterns and fraud detection logic

### 5. Data Generation Details

#### Per Thread Output with Sequential Numbers
- **authorization**: 500,000 rows with sequential sequence_numbers → parquet file
- **authorization_hash**: 500,000 rows with matching sequence_numbers → parquet file
- **clearing**: 500,000 rows with sequential sequence_numbers → parquet file
- **clearing_hash**: 500,000 rows with matching sequence_numbers → parquet file
- **chargeback**: ~500 rows referencing auth sequence_numbers → varies (0.1% of auth transactions)
- **chargeback_hash**: ~500 rows with matching sequence_numbers → varies (0.1% of auth transactions)

#### File Naming Convention
```
Combined Bucket: s3://bucket/table_name/yyyy/mm/dd/job_{job_index}_thread_{thread_id}.parquet
Specialized Buckets: s3://specialized-bucket/table_name/yyyy/mm/dd/job_{job_index}_thread_{thread_id}.parquet
```

Example:
```
Combined:
s3://combined-bucket/authorization/2023/05/15/job_12345_thread_1.parquet
s3://combined-bucket/authorization_hash/2023/05/15/job_12345_thread_1.parquet

Specialized:
s3://authorization-bucket/authorization/2023/05/15/job_12345_thread_1.parquet
s3://authorization-bucket/authorization_hash/2023/05/15/job_12345_thread_1.parquet
```

### 6. Scaling Configuration

#### Job Submitter Parameters
```rust
const MAX_ARRAY_SIZE: i32 = 1000;        // Jobs per array submission
const NUM_OF_ROWS: i32 = 1000000;         // Rows per thread
const CONCURRENT_SUBMISSIONS: usize = 50; // Parallel API calls
const BASE_TB: i32 = 655;                // Baseline for TB calculations
// Total jobs calculated dynamically: base_jobs × (TARGET_TB / BASE_TB)
// where base_jobs = 1,107,000 / 3 = 369,000
```

#### Container Resources
- **vCPUs**: 4 per container (optimized for 3 threads)
- **Memory**: 28GB per container
- **Threads**: 3 per container
- **Throughput**: 3x data generation per container

### 7. Monitoring and Troubleshooting

#### AWS Batch Console
- **Job Queue**: Monitor job submission and execution status
- **Job Details**: View individual job logs and resource usage
- **Compute Environment**: Monitor vCPU utilization and scaling

#### CloudWatch Logs
- **Log Group**: `/aws/batch/job`
- **Log Streams**: One per job execution
- **Key Metrics**: Generation time, upload success, file sizes, sequence number ranges

#### S3 Console
- **Bucket Structure**: 
  - Combined bucket: table_name/yyyy/mm/dd/
  - Specialized buckets: table_name/yyyy/mm/dd/ (clearing, authorization, chargeback)
- **File Verification**: Check file existence and sizes in both bucket types
- **Progress Tracking**: Monitor total data volume generated across all buckets

#### DynamoDB Console
- **Hash PAN Table**: 100k Mastercard PAN records with SHA256 hashes
- **Partition Counter Table**: Monitor atomic counters per partition date
  ```
  partition_date     | job_counter
  "2023-05-15"      | 127
  "2023-05-16"      | 89
  "2023-05-17"      | 203
  ```

### 8. Data Validation

#### Sequence Number Validation
- **Sequential Check**: Verify sequence_numbers increment by 1 within each partition
- **No Gaps**: Ensure no missing sequence numbers within job allocations
- **No Overlaps**: Verify different jobs have non-overlapping sequence ranges
- **Starting Point**: Each partition begins at 1000000000000001

#### File Size Targets
- **authorization/clearing**: Varies based on schema complexity and compression
- **authorization_hash/clearing_hash**: Smaller due to fewer fields (4 fields each)
- **chargeback/chargeback_hash**: Varies based on 0.1% selection

#### Business Logic Validation
- **Transaction Distribution**: Verify 85% PURCHASE, 5% CASH_ADVANCE, etc.
- **Card Network Accuracy**: Check Visa/Mastercard/Amex mapping
- **Field Consistency**: Validate MTI, response codes, status alignment

#### Data Consistency Checks
- All tables within a thread share same `hash_pan`
- Deterministic partition dates based on job_index
- Chargeback tables only exist for ~0.1% of threads
- Sequence numbers properly allocated across threads

### 9. Cost Optimization

#### Spot Instance Usage
- 50% spot instances for cost savings
- Automatic retry on spot interruptions
- Deterministic job execution for safe retries

#### Resource Efficiency
- Multi-threaded containers maximize vCPU utilization
- Parallel uploads minimize job duration
- Snappy compression reduces storage costs
- Atomic coordination eliminates duplicate work

### 10. Cleanup Process

#### Destroy Infrastructure
```bash
make destroy
```
**Preserves**: S3 bucket with generated data
**Removes**: Batch, ECR, VPC, DynamoDB tables, Lambda

#### Complete Cleanup
```bash
make destroy-data-bucket  # Optional: removes all generated data
```

## Performance Expectations

### Timeline
- **Infrastructure Deploy**: ~5-7 minutes
- **Job Submission (CodeDeploy)**: ~5-10 minutes
- **Data Generation**: Varies based on TARGET_TB configuration
- **Total Runtime**: Depends on data volume and cluster capacity

### Throughput
- **Per Container**: 3x baseline throughput (3 threads)
- **Per Thread**: 1,000,000 rows across 6 tables with sequential numbering
- **Total Output**: Configurable via TARGET_TB parameter in job-deployment-parameters.json

### Cost Estimate
- **Compute**: Varies based on TARGET_TB and runtime
- **Storage**: ~$23/month per TB in S3 Standard
- **DynamoDB**: Minimal cost for atomic counter operations + hash PAN storage

## Key Architectural Decisions

### Sequential Numbering Benefits
1. **Perfect Ordering**: No gaps or overlaps in sequence numbers
2. **Atomic Coordination**: DynamoDB ensures thread-safe allocation
3. **Partition Isolation**: Each partition date starts fresh from 1000000000000001
4. **Deterministic Results**: Same job always gets same sequence range

### Business Logic Realism
1. **Transaction Accuracy**: 85% purchases matches real payment processing
2. **Card Network Mapping**: Accurate Visa/Mastercard/Amex relationships
3. **Field Consistency**: MTI, response codes, status fields logically aligned
4. **Risk Patterns**: Amount-based fraud detection and alert patterns

### Multi-threading Benefits
1. **3x Throughput**: Each container generates 3x more data
2. **Unique Data**: Each thread gets different DynamoDB hash_pan
3. **Independent Logic**: Each thread makes its own chargeback decision
4. **Parallel Uploads**: All threads upload simultaneously

### Thread Independence
- No coordination between threads within a job
- Each thread handles complete generation + upload lifecycle
- Failure of one thread doesn't affect others
- Optimal resource utilization per container

### Deterministic Partitioning
- Same job_index always generates same partition date
- Safe for spot instance interruptions and retries
- Consistent file placement across runs
- Atomic sequence coordination prevents conflicts
