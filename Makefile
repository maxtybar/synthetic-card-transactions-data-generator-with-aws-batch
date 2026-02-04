# Makefile for building and deploying the data generation project

CDK_OUTPUTS_FILE := cdk-outputs.json
ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text)
REGION := $(shell aws configure get region)
PAYMENT_DATA_BUCKET := $(ACCOUNT_ID)-payment-data
CLEARING_BUCKET := $(ACCOUNT_ID)-clearing-data
AUTHORIZATION_BUCKET := $(ACCOUNT_ID)-authorization-data
CHARGEBACK_BUCKET := $(ACCOUNT_ID)-chargeback-data

.PHONY: help setup deploy clean destroy destroy-data-buckets trigger-generator-job

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  setup     							Install all dependencies (npm, rust)."
	@echo "  deploy    							Deploy infrastructure and automatically start data generation."
	@echo "  trigger-generator-job				Trigger new data generation jobs with current config."
	@echo "  destroy-data-buckets 				Empty and delete all S3 buckets with generated data."
	@echo "  destroy   							Tear down all AWS resources."
	@echo "  clean     							Remove local build artifacts."

setup:
	@echo ">>> Installing Node.js dependencies..."
	npm install
	@echo ">>> Bootstrapping CDK..."
	cdk bootstrap

deploy:
	@echo ">>> Deploying CDK stack with automatic build and job submission..."
	cdk deploy --outputs-file $(CDK_OUTPUTS_FILE) --require-approval never
	@echo ">>> Deployment complete. Data generation started automatically."
	@echo ">>> Monitor progress in AWS Batch and CodeBuild consoles."

destroy-data-buckets:
	@echo ">>> Emptying payment data bucket: $(PAYMENT_DATA_BUCKET)"
	aws s3 rm s3://$(PAYMENT_DATA_BUCKET) --recursive
	@echo ">>> Deleting payment data bucket: $(PAYMENT_DATA_BUCKET)"
	aws s3 rb s3://$(PAYMENT_DATA_BUCKET)
	@echo ">>> Emptying clearing bucket: $(CLEARING_BUCKET)"
	aws s3 rm s3://$(CLEARING_BUCKET) --recursive
	@echo ">>> Deleting clearing bucket: $(CLEARING_BUCKET)"
	aws s3 rb s3://$(CLEARING_BUCKET)
	@echo ">>> Emptying authorization bucket: $(AUTHORIZATION_BUCKET)"
	aws s3 rm s3://$(AUTHORIZATION_BUCKET) --recursive
	@echo ">>> Deleting authorization bucket: $(AUTHORIZATION_BUCKET)"
	aws s3 rb s3://$(AUTHORIZATION_BUCKET)
	@echo ">>> Emptying chargeback bucket: $(CHARGEBACK_BUCKET)"
	aws s3 rm s3://$(CHARGEBACK_BUCKET) --recursive
	@echo ">>> Deleting chargeback bucket: $(CHARGEBACK_BUCKET)"
	aws s3 rb s3://$(CHARGEBACK_BUCKET)
	@echo ">>> All data buckets destroyed."

destroy:
	@echo ">>> Tearing down the CDK stack..."
	@echo ">>> WARNING: This will delete all resources except the S3 buckets with data. Run 'make destroy-data-buckets' to destroy buckets with data separately."
	cdk destroy --force
	@echo ">>> Stack destruction initiated."

trigger-generator-job:
	@echo ">>> Reading job configuration..."
	@TARGET_TB=$$(jq -r '.TARGET_TB' apps/job-deployment-parameters.json); \
	INITIAL_LOAD=$$(jq -r '.INITIAL_LOAD' apps/job-deployment-parameters.json); \
	echo "Configuration: TARGET_TB=$$TARGET_TB, INITIAL_LOAD=$$INITIAL_LOAD"; \
	echo ">>> Triggering CodeBuild with current configuration..."; \
	BUILD_ID=$$(aws codebuild start-build \
		--project-name $(ACCOUNT_ID)-transactions-generator-build-and-submit \
		--environment-variables-override \
			name=TARGET_TB,value=$$TARGET_TB \
			name=INITIAL_LOAD,value=$$INITIAL_LOAD \
		--query 'build.id' --output text); \
	echo ">>> CodeBuild started: $$BUILD_ID"; \
	echo ">>> Monitor at: https://console.aws.amazon.com/codesuite/codebuild/projects/$(ACCOUNT_ID)-transactions-generator-build-and-submit/build/$$BUILD_ID"

clean:
	@echo ">>> Cleaning up local artifacts..."
	rm -f source.zip
	rm -f $(CDK_OUTPUTS_FILE)
	cd apps/data-generator && cargo clean
	cd apps/job-submitter && cargo clean
	@echo ">>> Done."
