import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import * as batch from 'aws-cdk-lib/aws-batch';
import * as codebuild from 'aws-cdk-lib/aws-codebuild';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { BlockPublicAccess, BucketEncryption } from 'aws-cdk-lib/aws-s3';
import * as fs from 'fs';
import * as path from 'path';
import { GlueTablesConstruct } from './glue-tables-construct';

export class BatchDataGeneratorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Read job deployment parameters
    const configPath = path.join(__dirname, '..', 'apps', 'job-deployment-parameters.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    const targetTB = config.TARGET_TB;
    const initialLoad = config.INITIAL_LOAD;
    const chargebackPercentage = config.CHARGEBACK_PERCENTAGE;
    const cardBrand = config.CARD_BRAND;
    const networkBrand = config.NETWORK_BRAND;

    // === Part 1: Build Infrastructure ===

    const sourceBucket = new s3.Bucket(this, 'BuildSourceBucket', {
      bucketName: `${this.account}-build-source`,
      versioned: false,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const ecrRepo = new ecr.Repository(this, 'DataGenEcrRepo', {
      repositoryName: `${this.account}-data-generator-repository`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      emptyOnDelete: true,
    });

    // === Part 2: AWS Batch Infrastructure ===

    // This VPC has larger subnets and VPC endpoints for ECR access
    const vpc = new ec2.Vpc(this, 'DataGenVPC', {
      maxAzs: 3,
      natGateways: 0,
      restrictDefaultSecurityGroup: false,
      subnetConfiguration: [{
          cidrMask: 20,
          name: 'Isolated',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      }],
    });

    // Add VPC endpoints for ECR access from isolated subnets
    vpc.addGatewayEndpoint('S3Endpoint', {
        service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // ECR API endpoint for pulling images
    const ecrApiEndpoint = vpc.addInterfaceEndpoint('EcrApiEndpoint', {
        service: ec2.InterfaceVpcEndpointAwsService.ECR,
    });
    ecrApiEndpoint.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    // ECR Docker endpoint for pulling Docker layers
    const ecrDockerEndpoint = vpc.addInterfaceEndpoint('EcrDockerEndpoint', {
        service: ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
    });
    ecrDockerEndpoint.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    // CloudWatch Logs endpoint for logging
    const cloudWatchLogsEndpoint = vpc.addInterfaceEndpoint('CloudWatchLogsEndpoint', {
        service: ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
    });
    cloudWatchLogsEndpoint.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    // DynamoDB gateway endpoint for private access
    vpc.addGatewayEndpoint('DynamoDbEndpoint', {
        service: ec2.GatewayVpcEndpointAwsService.DYNAMODB,
    });

    const paymentDataBucket = new s3.Bucket(this, 'PaymentDataBucket', {
      bucketName: `${this.account}-payment-data`,
      versioned: false,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const clearingBucket = new s3.Bucket(this, 'ClearingDataBucket', {
      bucketName: `${this.account}-clearing-data`,
      versioned: false,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const authorizationBucket = new s3.Bucket(this, 'AuthorizationDataBucket', {
      bucketName: `${this.account}-authorization-data`,
      versioned: false,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const chargebackBucket = new s3.Bucket(this, 'ChargebackDataBucket', {
      bucketName: `${this.account}-chargeback-data`,
      versioned: false,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const athenaQueryResultsBucket = new s3.Bucket(this, 'AthenaQueryResultsBucket', {
      bucketName: `${this.account}-athena-query-results`,
      versioned: false,
      encryption: BucketEncryption.S3_MANAGED,
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // Create Glue Database and Tables
    const glueTables = new GlueTablesConstruct(this, 'GlueTables', {
      paymentDataBucket,
      authorizationBucket,
      clearingBucket,
      chargebackBucket,
    });

    // DynamoDB table for hash_pan values
    const hashPanTable = new dynamodb.Table(this, 'HashPanTable', {
      tableName: `${this.account}-hashed-pan-table`,
      partitionKey: { name: 'id', type: dynamodb.AttributeType.NUMBER },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // DynamoDB table for partition sequence counters
    const partitionCounterTable = new dynamodb.Table(this, 'PartitionCounterTable', {
      tableName: `${this.account}-partition-counter-table`,
      partitionKey: { name: 'partition_date', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const jobRole = new iam.Role(this, 'BatchJobRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
    });
    paymentDataBucket.grantWrite(jobRole);
    clearingBucket.grantWrite(jobRole);
    authorizationBucket.grantWrite(jobRole);
    chargebackBucket.grantWrite(jobRole);
    hashPanTable.grantReadData(jobRole);
    partitionCounterTable.grantReadWriteData(jobRole);
    
    // Add KMS permissions for S3 encryption
    jobRole.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'kms:GenerateDataKey',
        'kms:Decrypt',
        'kms:DescribeKey'
      ],
      resources: ['*'], // S3 managed keys
    }));
    
    hashPanTable.grantReadData(jobRole);

    // Execution role for Fargate tasks
    const executionRole = new iam.Role(this, 'ContainerDefExecutionRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonECSTaskExecutionRolePolicy'),
      ],
    });

    ecrRepo.grantPull(executionRole);

    const timestamp = new Date().toISOString().slice(0, 16).replace(/[-:T]/g, '-');
    
    // 2 compute environments for flexible resource allocation
    const spotComputeEnv = new batch.FargateComputeEnvironment(this, 'SpotComputeEnv', {
      computeEnvironmentName: `${this.account}-spot-compute-env-${timestamp}`,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      maxvCpus: 4000,  // All spot vCPUs
      spot: true,
      
    });

    const onDemandComputeEnv = new batch.FargateComputeEnvironment(this, 'OnDemandComputeEnv', {
      computeEnvironmentName: `${this.account}-ondemand-compute-env-${timestamp}`,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      maxvCpus: 4000,  // All on-demand vCPUs
      spot: false,
    });
    
    // 2 queues with alternating priority for maximum utilization
    const spotQueue = new batch.JobQueue(this, 'SpotJobQueue', {
      jobQueueName: `${this.account}-spot-queue-${timestamp}`,
      priority: 100,
      computeEnvironments: [
        { computeEnvironment: spotComputeEnv, order: 1 },
        { computeEnvironment: onDemandComputeEnv, order: 2 },
      ],
    });

    const onDemandQueue = new batch.JobQueue(this, 'OnDemandJobQueue', {
      jobQueueName: `${this.account}-ondemand-queue-${timestamp}`,
      priority: 100,
      computeEnvironments: [
        { computeEnvironment: onDemandComputeEnv, order: 1 },
        { computeEnvironment: spotComputeEnv, order: 2 },
      ],
    });

    // Job definition with container specs
    const jobDefinition = new batch.EcsJobDefinition(this, 'DataGenJobDefinition', {
      jobDefinitionName: `${this.account}-transactions-generator-jobdef-${timestamp}`,
      retryAttempts: 3,
      container: new batch.EcsFargateContainerDefinition(this, 'ContainerDef', {
        image: ecs.ContainerImage.fromEcrRepository(ecrRepo),
        memory: cdk.Size.gibibytes(28),
        cpu: 4,                           
        jobRole: jobRole,
        executionRole: executionRole,
        assignPublicIp: false,
        logging: ecs.LogDrivers.awsLogs({
          streamPrefix: `${this.account}-batch-data-generator`,
          logRetention: logs.RetentionDays.ONE_WEEK,
        }),
      }),
    });

    // Deploy source code to S3
    const sourceDeployment = new s3deploy.BucketDeployment(this, 'DeploySource', {
      sources: [s3deploy.Source.asset('apps')],
      destinationBucket: sourceBucket,
      destinationKeyPrefix: '',
      extract: false,
      prune: false,
    });

    const buildProject = new codebuild.Project(this, 'DataGenBuildProject', {
      projectName: `${this.account}-transactions-generator-build-and-submit`,
      source: codebuild.Source.s3({
        bucket: sourceBucket,
        path: cdk.Fn.select(0, sourceDeployment.objectKeys),
      }),
      environment: {
        buildImage: codebuild.LinuxBuildImage.STANDARD_7_0,
        computeType: codebuild.ComputeType.X_LARGE,
        privileged: true,
      },
      environmentVariables: {
        ECR_REPO_URI: { value: ecrRepo.repositoryUri },
        AWS_ACCOUNT_ID: { value: cdk.Aws.ACCOUNT_ID },
        AWS_DEFAULT_REGION: { value: cdk.Aws.REGION },
        SPOT_QUEUE_NAME: { value: spotQueue.jobQueueName },
        ONDEMAND_QUEUE_NAME: { value: onDemandQueue.jobQueueName },
        JOB_DEFINITION_ARN: { value: jobDefinition.jobDefinitionArn },
        PAYMENT_DATA_BUCKET_NAME: { value: paymentDataBucket.bucketName },
        CLEARING_BUCKET_NAME: { value: clearingBucket.bucketName },
        AUTHORIZATION_BUCKET_NAME: { value: authorizationBucket.bucketName },
        CHARGEBACK_BUCKET_NAME: { value: chargebackBucket.bucketName },
        HASH_PAN_TABLE_NAME: { value: hashPanTable.tableName },
        PARTITION_COUNTER_TABLE_NAME: { value: partitionCounterTable.tableName },
        TARGET_TB: { value: targetTB.toString() },
        INITIAL_LOAD: { value: initialLoad.toString() },
        CHARGEBACK_PERCENTAGE: { value: chargebackPercentage.toString() },
        CARD_BRAND: { value: cardBrand },
        NETWORK_BRAND: { value: networkBrand },
      },
      buildSpec: codebuild.BuildSpec.fromSourceFilename('buildspec.yml'),
    });

    ecrRepo.grantPullPush(buildProject.role!);
    sourceBucket.grantRead(buildProject.role!);
    hashPanTable.grantWriteData(buildProject.role!);
    
    // Grant Batch permissions for single queue
    buildProject.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['batch:SubmitJob'],
      resources: [
        spotQueue.jobQueueArn, 
        onDemandQueue.jobQueueArn,
        jobDefinition.jobDefinitionArn
      ],
    }));

    // Lambda function to wait then trigger CodeBuild
    const triggerLambda = new lambda.Function(this, 'TriggerBuildLambda', {
      runtime: lambda.Runtime.PYTHON_3_11,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
import boto3
import time
import json

def handler(event, context):
    print("Waiting 30 seconds for IAM propagation...")
    time.sleep(30)
    
    codebuild = boto3.client('codebuild')
    response = codebuild.start_build(
        projectName='${buildProject.projectName}'
    )
    
    print(f"Started CodeBuild: {response['build']['id']}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({'buildId': response['build']['id']})
    }
      `),
      timeout: cdk.Duration.minutes(5),
    });

    // Grant Lambda permission to start CodeBuild
    triggerLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: ['codebuild:StartBuild'],
      resources: [buildProject.projectArn],
    }));

    // Trigger the Lambda after deployment
    const triggerBuild = new cr.AwsCustomResource(this, 'TriggerCodeBuild', {
      onCreate: {
        service: 'Lambda',
        action: 'invoke',
        parameters: {
          FunctionName: triggerLambda.functionName,
        },
        physicalResourceId: cr.PhysicalResourceId.of('trigger-build'),
      },
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({
          actions: ['lambda:InvokeFunction'],
          resources: [triggerLambda.functionArn],
        }),
      ]),
      installLatestAwsSdk: false,
    });

    // Ensure build is triggered after all batch resources and source deployment
    triggerBuild.node.addDependency(sourceDeployment);
    triggerBuild.node.addDependency(spotQueue);
    triggerBuild.node.addDependency(onDemandQueue);
    triggerBuild.node.addDependency(jobDefinition);

    // === Outputs ===
    new cdk.CfnOutput(this, 'CodeBuildProjectName', { value: buildProject.projectName });
    new cdk.CfnOutput(this, 'SourceBucketName', { value: sourceBucket.bucketName });
    new cdk.CfnOutput(this, 'PaymentDataBucketName', { value: paymentDataBucket.bucketName });
    new cdk.CfnOutput(this, 'ClearingBucketName', { value: clearingBucket.bucketName });
    new cdk.CfnOutput(this, 'AuthorizationBucketName', { value: authorizationBucket.bucketName });
    new cdk.CfnOutput(this, 'ChargebackBucketName', { value: chargebackBucket.bucketName });
    new cdk.CfnOutput(this, 'AthenaQueryResultsBucketName', { value: athenaQueryResultsBucket.bucketName });
    new cdk.CfnOutput(this, 'HashPanTableName', { value: hashPanTable.tableName });
    new cdk.CfnOutput(this, 'PartitionCounterTableName', { value: partitionCounterTable.tableName });
    new cdk.CfnOutput(this, 'SpotJobQueueArn', { value: spotQueue.jobQueueArn });
    new cdk.CfnOutput(this, 'OnDemandJobQueueArn', { value: onDemandQueue.jobQueueArn });
    new cdk.CfnOutput(this, 'BatchJobDefinitionArn', { value: jobDefinition.jobDefinitionArn });
  }
}

