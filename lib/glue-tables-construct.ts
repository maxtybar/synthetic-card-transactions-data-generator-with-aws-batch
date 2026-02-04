import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as fs from 'fs';
import * as path from 'path';

export interface GlueTablesConstructProps {
  paymentDataBucket: s3.IBucket;
  authorizationBucket: s3.IBucket;
  clearingBucket: s3.IBucket;
  chargebackBucket: s3.IBucket;
}

export class GlueTablesConstruct extends Construct {
  public readonly database: glue.CfnDatabase;

  constructor(scope: Construct, id: string, props: GlueTablesConstructProps) {
    super(scope, id);

    // Create Glue Database
    this.database = new glue.CfnDatabase(this, 'PaymentDataDatabase', {
      catalogId: cdk.Stack.of(this).account,
      databaseInput: {
        name: 'payment_data',
        description: '6-table payment processing data: authorization, authorization_hash, clearing, clearing_hash, chargeback, chargeback_hash',
        locationUri: props.paymentDataBucket.s3UrlForObject(),
      },
    });

    // Read authorization schema
    const authSchemaPath = path.join(__dirname, '..', 'apps', 'data-generator', 'schemas', 'authorization_schema.json');
    const authSchema = JSON.parse(fs.readFileSync(authSchemaPath, 'utf8'));

    // Create Authorization Table
    new glue.CfnTable(this, 'AuthorizationTable', {
      catalogId: cdk.Stack.of(this).account,
      databaseName: this.database.ref,
      tableInput: {
        name: 'authorization',
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
        storageDescriptor: {
          columns: this.convertSchemaToColumns(authSchema),
          location: `${props.authorizationBucket.s3UrlForObject()}/authorization/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        parameters: {
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2026',
          'projection.year.digits': '4',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `${props.authorizationBucket.s3UrlForObject()}/authorization/\${year}/\${month}/\${day}/`,
        },
      },
    });

    // Create Authorization Hash Table
    new glue.CfnTable(this, 'AuthorizationHashTable', {
      catalogId: cdk.Stack.of(this).account,
      databaseName: this.database.ref,
      tableInput: {
        name: 'authorization_hash',
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
        storageDescriptor: {
          columns: [
            { name: 'hash_pan', type: 'string' },
            { name: 'sequence_number', type: 'bigint' },
            { name: 'process_date', type: 'timestamp' },
            { name: 'insert_date', type: 'timestamp' },
          ],
          location: `${props.authorizationBucket.s3UrlForObject()}/authorization_hash/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        parameters: {
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2026',
          'projection.year.digits': '4',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `${props.authorizationBucket.s3UrlForObject()}/authorization_hash/\${year}/\${month}/\${day}/`,
        },
      },
    });

    // Create Clearing Table (similar structure, read from clearing_schema.json)
    const clearingSchemaPath = path.join(__dirname, '..', 'apps', 'data-generator', 'schemas', 'clearing_schema.json');
    const clearingSchema = JSON.parse(fs.readFileSync(clearingSchemaPath, 'utf8'));

    new glue.CfnTable(this, 'ClearingTable', {
      catalogId: cdk.Stack.of(this).account,
      databaseName: this.database.ref,
      tableInput: {
        name: 'clearing',
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
        storageDescriptor: {
          columns: this.convertSchemaToColumns(clearingSchema),
          location: `${props.clearingBucket.s3UrlForObject()}/clearing/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        parameters: {
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2026',
          'projection.year.digits': '4',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `${props.clearingBucket.s3UrlForObject()}/clearing/\${year}/\${month}/\${day}/`,
        },
      },
    });

    // Create Clearing Hash Table
    new glue.CfnTable(this, 'ClearingHashTable', {
      catalogId: cdk.Stack.of(this).account,
      databaseName: this.database.ref,
      tableInput: {
        name: 'clearing_hash',
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
        storageDescriptor: {
          columns: [
            { name: 'hash_pan', type: 'string' },
            { name: 'sequence_number', type: 'bigint' },
            { name: 'process_date', type: 'timestamp' },
            { name: 'insert_date', type: 'timestamp' },
          ],
          location: `${props.clearingBucket.s3UrlForObject()}/clearing_hash/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        parameters: {
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2026',
          'projection.year.digits': '4',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `${props.clearingBucket.s3UrlForObject()}/clearing_hash/\${year}/\${month}/\${day}/`,
        },
      },
    });

    // Create Chargeback Table
    const chargebackSchemaPath = path.join(__dirname, '..', 'apps', 'data-generator', 'schemas', 'chargeback_schema.json');
    const chargebackSchema = JSON.parse(fs.readFileSync(chargebackSchemaPath, 'utf8'));

    new glue.CfnTable(this, 'ChargebackTable', {
      catalogId: cdk.Stack.of(this).account,
      databaseName: this.database.ref,
      tableInput: {
        name: 'chargeback',
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
        storageDescriptor: {
          columns: this.convertSchemaToColumns(chargebackSchema),
          location: `${props.chargebackBucket.s3UrlForObject()}/chargeback/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        parameters: {
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2026',
          'projection.year.digits': '4',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `${props.chargebackBucket.s3UrlForObject()}/chargeback/\${year}/\${month}/\${day}/`,
        },
      },
    });

    // Create Chargeback Hash Table
    new glue.CfnTable(this, 'ChargebackHashTable', {
      catalogId: cdk.Stack.of(this).account,
      databaseName: this.database.ref,
      tableInput: {
        name: 'chargeback_hash',
        tableType: 'EXTERNAL_TABLE',
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
        ],
        storageDescriptor: {
          columns: [
            { name: 'hash_pan', type: 'string' },
            { name: 'sequence_number', type: 'bigint' },
            { name: 'process_date', type: 'timestamp' },
            { name: 'insert_date', type: 'timestamp' },
          ],
          location: `${props.chargebackBucket.s3UrlForObject()}/chargeback_hash/`,
          inputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
          },
        },
        parameters: {
          'projection.enabled': 'true',
          'projection.year.type': 'integer',
          'projection.year.range': '2020,2026',
          'projection.year.digits': '4',
          'projection.month.type': 'integer',
          'projection.month.range': '01,12',
          'projection.month.digits': '2',
          'projection.day.type': 'integer',
          'projection.day.range': '01,31',
          'projection.day.digits': '2',
          'storage.location.template': `${props.chargebackBucket.s3UrlForObject()}/chargeback_hash/\${year}/\${month}/\${day}/`,
        },
      },
    });
  }

  private convertSchemaToColumns(schema: any): glue.CfnTable.ColumnProperty[] {
    const columns: glue.CfnTable.ColumnProperty[] = [];

    // Map schema types to Glue types
    const typeMap: { [key: string]: string } = {
      strings: 'string',
      ints: 'int',
      bigints: 'bigint',
      smallints: 'smallint',
      tinyints: 'tinyint',
      timestamps: 'timestamp',
    };

    // Handle regular types
    for (const [fieldType, fieldList] of Object.entries(schema.fields)) {
      if (typeMap[fieldType]) {
        (fieldList as string[]).forEach((fieldName: string) => {
          columns.push({ name: fieldName, type: typeMap[fieldType] });
        });
      } else if (fieldType.startsWith('decimals_')) {
        // Handle decimal types (e.g., decimals_18_2 -> decimal(18,2))
        const parts = fieldType.replace('decimals_', '').split('_');
        const precision = parts[0];
        const scale = parts[1];
        (fieldList as string[]).forEach((fieldName: string) => {
          columns.push({ name: fieldName, type: `decimal(${precision},${scale})` });
        });
      }
    }

    return columns;
  }
}
