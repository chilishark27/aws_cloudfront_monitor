#!/bin/bash
#
# Package Lambda deployment artifact for CloudFront Abuse Detection System
#
# This script creates a deployment.zip file containing:
# - src/ directory (all existing modules)
# - scheduler_handler.py (Scheduler Lambda entry point)
# - worker_handler.py (Worker Lambda entry point)
# - Python dependencies from requirements-prod.txt (excluding boto3, botocore)
#
# Usage: ./scripts/package.sh
#
# Output: deployment.zip in the project root directory
#
# Requirements:
# - Python 3.12 (or compatible version)
# - pip
# - zip
#
# Validates: Requirements 8.1, 8.3, 8.4

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Output file
OUTPUT_FILE="${PROJECT_ROOT}/deployment.zip"

# Temporary directory for packaging
TEMP_DIR=$(mktemp -d)

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Lambda Deployment Package Builder${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Project root: ${PROJECT_ROOT}"
echo "Temp directory: ${TEMP_DIR}"
echo "Output file: ${OUTPUT_FILE}"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo -e "${YELLOW}Cleaning up temporary directory...${NC}"
    rm -rf "${TEMP_DIR}"
    echo -e "${GREEN}Cleanup complete.${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Step 1: Verify required files exist
echo -e "${YELLOW}Step 1: Verifying required files...${NC}"

if [ ! -f "${PROJECT_ROOT}/requirements-prod.txt" ]; then
    echo -e "${RED}Error: requirements-prod.txt not found${NC}"
    exit 1
fi

if [ ! -d "${PROJECT_ROOT}/src" ]; then
    echo -e "${RED}Error: src/ directory not found${NC}"
    exit 1
fi

if [ ! -f "${PROJECT_ROOT}/scheduler_handler.py" ]; then
    echo -e "${RED}Error: scheduler_handler.py not found${NC}"
    exit 1
fi

if [ ! -f "${PROJECT_ROOT}/worker_handler.py" ]; then
    echo -e "${RED}Error: worker_handler.py not found${NC}"
    exit 1
fi

echo -e "${GREEN}All required files found.${NC}"
echo ""

# Step 2: Install Python dependencies (excluding boto3 and botocore)
echo -e "${YELLOW}Step 2: Installing Python dependencies...${NC}"

# Create a filtered requirements file excluding boto3 and botocore
FILTERED_REQUIREMENTS="${TEMP_DIR}/requirements-filtered.txt"
grep -v -E '^(boto3|botocore)' "${PROJECT_ROOT}/requirements-prod.txt" | grep -v '^#' | grep -v '^$' > "${FILTERED_REQUIREMENTS}" || true

if [ -s "${FILTERED_REQUIREMENTS}" ]; then
    echo "Installing dependencies from filtered requirements:"
    cat "${FILTERED_REQUIREMENTS}"
    echo ""
    
    pip install \
        --target "${TEMP_DIR}/package" \
        --requirement "${FILTERED_REQUIREMENTS}" \
        --quiet \
        --no-cache-dir
    
    echo -e "${GREEN}Dependencies installed successfully.${NC}"
else
    echo "No additional dependencies to install (boto3/botocore excluded)."
    mkdir -p "${TEMP_DIR}/package"
fi
echo ""

# Step 3: Copy src directory
echo -e "${YELLOW}Step 3: Copying src/ directory...${NC}"

cp -r "${PROJECT_ROOT}/src" "${TEMP_DIR}/package/"

# Remove __pycache__ directories
find "${TEMP_DIR}/package/src" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

echo -e "${GREEN}src/ directory copied.${NC}"
echo ""

# Step 4: Copy Lambda handler files
echo -e "${YELLOW}Step 4: Copying Lambda handler files...${NC}"

cp "${PROJECT_ROOT}/scheduler_handler.py" "${TEMP_DIR}/package/"
cp "${PROJECT_ROOT}/worker_handler.py" "${TEMP_DIR}/package/"

echo -e "${GREEN}Handler files copied:${NC}"
echo "  - scheduler_handler.py"
echo "  - worker_handler.py"
echo ""

# Step 5: Create deployment.zip
echo -e "${YELLOW}Step 5: Creating deployment.zip...${NC}"

# Remove existing deployment.zip if it exists
if [ -f "${OUTPUT_FILE}" ]; then
    echo "Removing existing deployment.zip..."
    rm -f "${OUTPUT_FILE}"
fi

# Create zip file from package directory
cd "${TEMP_DIR}/package"
zip -r -q "${OUTPUT_FILE}" .

echo -e "${GREEN}deployment.zip created successfully.${NC}"
echo ""

# Step 6: Display package information
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Package Summary${NC}"
echo -e "${GREEN}========================================${NC}"

# Get file size
if [ -f "${OUTPUT_FILE}" ]; then
    FILE_SIZE=$(du -h "${OUTPUT_FILE}" | cut -f1)
    echo "Output file: ${OUTPUT_FILE}"
    echo "Package size: ${FILE_SIZE}"
    echo ""
    
    # List package contents (top-level only)
    echo "Package contents (top-level):"
    unzip -l "${OUTPUT_FILE}" | head -20
    echo ""
    
    # Count files
    FILE_COUNT=$(unzip -l "${OUTPUT_FILE}" | tail -1 | awk '{print $2}')
    echo "Total files: ${FILE_COUNT}"
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Packaging complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Upload to S3: aws s3 cp deployment.zip s3://your-bucket/your-key.zip"
echo "  2. Deploy CloudFormation stack with S3Bucket and S3Key parameters"
echo ""
