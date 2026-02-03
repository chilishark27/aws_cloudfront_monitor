#!/bin/bash
#
# CloudFormation Deployment Script for CloudFront Abuse Detection System
#
# This script deploys the CloudFormation stack with all required resources:
# - Scheduler and Worker Lambda functions
# - DynamoDB tables
# - Secrets Manager secret
# - EventBridge rule
# - IAM roles and policies
#
# Usage: ./scripts/deploy.sh [options]
#
# Required Parameters (via environment variables or command line):
#   S3_BUCKET           - S3 bucket for Lambda deployment package
#   TELEGRAM_BOT_TOKEN  - Telegram bot token for alerts
#   TELEGRAM_CHAT_ID    - Telegram chat ID for alerts
#
# Optional Parameters:
#   STACK_NAME          - CloudFormation stack name (default: cloudfront-abuse-detection)
#   S3_KEY              - S3 key for deployment package (default: lambda/deployment.zip)
#   AWS_REGION          - AWS region (default: us-east-1)
#
# Validates: Requirements 8.1, 8.2

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default configuration
STACK_NAME="${STACK_NAME:-cloudfront-abuse-detection}"
S3_KEY="${S3_KEY:-lambda/deployment.zip}"
AWS_REGION="${AWS_REGION:-us-east-1}"
DEPLOYMENT_PACKAGE="${PROJECT_ROOT}/deployment.zip"
TEMPLATE_FILE="${PROJECT_ROOT}/cloudformation/template.yaml"

# Helper functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Display usage information
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "CloudFormation Deployment Script for CloudFront Abuse Detection System"
    echo ""
    echo "Required Environment Variables:"
    echo "  S3_BUCKET           S3 bucket for Lambda deployment package"
    echo "  TELEGRAM_BOT_TOKEN  Telegram bot token for alerts"
    echo "  TELEGRAM_CHAT_ID    Telegram chat ID for alerts"
    echo ""
    echo "Optional Environment Variables:"
    echo "  STACK_NAME          CloudFormation stack name (default: cloudfront-abuse-detection)"
    echo "  S3_KEY              S3 key for deployment package (default: lambda/deployment.zip)"
    echo "  AWS_REGION          AWS region (default: us-east-1)"
    echo ""
    echo "Examples:"
    echo "  # Basic deployment"
    echo "  S3_BUCKET=my-bucket TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy ./scripts/deploy.sh"
    echo ""
    echo "  # Custom stack name and region"
    echo "  STACK_NAME=my-stack AWS_REGION=eu-west-1 S3_BUCKET=my-bucket \\"
    echo "    TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=yyy ./scripts/deploy.sh"
    echo ""
    exit 1
}

# Validate required parameters
validate_parameters() {
    log_step "Validating Parameters"
    
    local missing_params=()
    
    if [ -z "${S3_BUCKET}" ]; then
        missing_params+=("S3_BUCKET")
    fi
    
    if [ -z "${TELEGRAM_BOT_TOKEN}" ]; then
        missing_params+=("TELEGRAM_BOT_TOKEN")
    fi
    
    if [ -z "${TELEGRAM_CHAT_ID}" ]; then
        missing_params+=("TELEGRAM_CHAT_ID")
    fi
    
    if [ ${#missing_params[@]} -gt 0 ]; then
        log_error "Missing required parameters: ${missing_params[*]}"
        echo ""
        usage
    fi
    
    log_info "Stack Name:     ${STACK_NAME}"
    log_info "S3 Bucket:      ${S3_BUCKET}"
    log_info "S3 Key:         ${S3_KEY}"
    log_info "AWS Region:     ${AWS_REGION}"
    log_info "Template File:  ${TEMPLATE_FILE}"
}

# Check prerequisites
check_prerequisites() {
    log_step "Checking Prerequisites"
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    log_info "AWS CLI found: $(aws --version)"
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials are not configured. Please run 'aws configure'."
        exit 1
    fi
    
    local account_id=$(aws sts get-caller-identity --query Account --output text)
    log_info "AWS Account: ${account_id}"
    
    # Check deployment package exists
    if [ ! -f "${DEPLOYMENT_PACKAGE}" ]; then
        log_error "Deployment package not found: ${DEPLOYMENT_PACKAGE}"
        log_error "Please run './scripts/package.sh' first to create the deployment package."
        exit 1
    fi
    log_info "Deployment package found: ${DEPLOYMENT_PACKAGE} ($(du -h ${DEPLOYMENT_PACKAGE} | cut -f1))"
    
    # Check CloudFormation template exists
    if [ ! -f "${TEMPLATE_FILE}" ]; then
        log_error "CloudFormation template not found: ${TEMPLATE_FILE}"
        exit 1
    fi
    log_info "CloudFormation template found: ${TEMPLATE_FILE}"
}

# Validate CloudFormation template
validate_template() {
    log_step "Validating CloudFormation Template"
    
    log_info "Running template validation..."
    
    if aws cloudformation validate-template \
        --template-body "file://${TEMPLATE_FILE}" \
        --region "${AWS_REGION}" > /dev/null 2>&1; then
        log_info "Template validation successful."
    else
        log_error "Template validation failed."
        aws cloudformation validate-template \
            --template-body "file://${TEMPLATE_FILE}" \
            --region "${AWS_REGION}"
        exit 1
    fi
}

# Upload deployment package to S3
upload_to_s3() {
    log_step "Uploading Deployment Package to S3"
    
    local s3_uri="s3://${S3_BUCKET}/${S3_KEY}"
    
    log_info "Uploading ${DEPLOYMENT_PACKAGE} to ${s3_uri}..."
    
    if aws s3 cp "${DEPLOYMENT_PACKAGE}" "${s3_uri}" --region "${AWS_REGION}"; then
        log_info "Upload successful."
        
        # Verify upload
        local s3_size=$(aws s3 ls "${s3_uri}" --region "${AWS_REGION}" | awk '{print $3}')
        local local_size=$(stat -f%z "${DEPLOYMENT_PACKAGE}" 2>/dev/null || stat -c%s "${DEPLOYMENT_PACKAGE}" 2>/dev/null)
        
        log_info "S3 object size: ${s3_size} bytes"
        log_info "Local file size: ${local_size} bytes"
    else
        log_error "Failed to upload deployment package to S3."
        exit 1
    fi
}

# Deploy CloudFormation stack
deploy_stack() {
    log_step "Deploying CloudFormation Stack"
    
    log_info "Stack Name: ${STACK_NAME}"
    log_info "Region: ${AWS_REGION}"
    
    # Check if stack exists
    local stack_exists=false
    if aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" &> /dev/null; then
        stack_exists=true
        log_info "Stack exists. Updating..."
    else
        log_info "Stack does not exist. Creating..."
    fi
    
    # Deploy stack using aws cloudformation deploy
    log_info "Running CloudFormation deploy..."
    
    aws cloudformation deploy \
        --template-file "${TEMPLATE_FILE}" \
        --stack-name "${STACK_NAME}" \
        --parameter-overrides \
            TelegramBotToken="${TELEGRAM_BOT_TOKEN}" \
            TelegramChatId="${TELEGRAM_CHAT_ID}" \
            S3Bucket="${S3_BUCKET}" \
            S3Key="${S3_KEY}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "${AWS_REGION}" \
        --no-fail-on-empty-changeset
    
    local deploy_status=$?
    
    if [ ${deploy_status} -eq 0 ]; then
        log_info "CloudFormation deployment completed successfully."
    else
        log_error "CloudFormation deployment failed with status: ${deploy_status}"
        exit 1
    fi
}

# Output deployment results
output_results() {
    log_step "Deployment Results"
    
    # Get stack status
    local stack_status=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].StackStatus' \
        --output text)
    
    log_info "Stack Status: ${stack_status}"
    
    # Get stack outputs
    log_info "Stack Outputs:"
    echo ""
    
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
        --output table
    
    echo ""
    
    # Get resource summary
    log_info "Created Resources:"
    echo ""
    
    aws cloudformation list-stack-resources \
        --stack-name "${STACK_NAME}" \
        --region "${AWS_REGION}" \
        --query 'StackResourceSummaries[*].[LogicalResourceId,ResourceType,ResourceStatus]' \
        --output table
    
    echo ""
    
    # Display next steps
    log_step "Deployment Complete!"
    
    echo ""
    echo "Next steps:"
    echo "  1. Verify Lambda functions in AWS Console"
    echo "  2. Check EventBridge rule is enabled"
    echo "  3. Test Scheduler Lambda manually:"
    echo "     aws lambda invoke --function-name ${STACK_NAME}-scheduler --region ${AWS_REGION} output.json"
    echo "  4. Monitor CloudWatch Logs for execution details"
    echo ""
    echo "Useful commands:"
    echo "  # View stack events"
    echo "  aws cloudformation describe-stack-events --stack-name ${STACK_NAME} --region ${AWS_REGION}"
    echo ""
    echo "  # Delete stack"
    echo "  aws cloudformation delete-stack --stack-name ${STACK_NAME} --region ${AWS_REGION}"
    echo ""
}

# Main deployment flow
main() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}CloudFormation Deployment Script${NC}"
    echo -e "${GREEN}CloudFront Abuse Detection System${NC}"
    echo -e "${GREEN}========================================${NC}"
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
        shift
    done
    
    # Run deployment steps
    validate_parameters
    check_prerequisites
    validate_template
    upload_to_s3
    deploy_stack
    output_results
}

# Run main function
main "$@"
