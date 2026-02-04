#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import 'source-map-support/register';
import { BatchDataGeneratorStack } from '../lib/batch-data-generator-stack';

const app = new cdk.App();
new BatchDataGeneratorStack(app, 'BatchDataGeneratorStack', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});
