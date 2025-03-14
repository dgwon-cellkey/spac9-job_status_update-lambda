#!/bin/bash
# Immediately terminate scripts when an error occurs
set -e

# Command to run shell script
# bash lambda_deploy.sh config_{account-alias}.sh

# Get Config
if [ -z "$1" ]; then
    echo "Usage: $0 <env_file>"
    exit 1
fi

if [ ! -f "$1" ]; then
    echo "Error: Environment file '$1' not found!"
    exit 1
fi

source "$1"

echo "Create lambda_package directory..."
mkdir -p lambda_package
cp lambda_function.py lambda_package/

echo "Installing dependencies..."
pip install pymysql -t lambda_package

echo "Creating zip file..."
cd lambda_package
zip -r ../$ZIP_FILE .
cd ..

echo "Verify that Lambda exist and proceed with Deployment"
if aws lambda get-function --function-name $FUNCTION_NAME >/dev/null 2>&1; then
    echo "Updating Lambda function..."
    aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://$ZIP_FILE
else
    echo "Creating new Lambda function..."
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --runtime $RUNTIME \
        --role $ROLE_ARN \
        --handler $HANDLER \
        --zip-file fileb://$ZIP_FILE
fi

if [ $? -eq 0 ]; then
    echo "Lambda deployed successfully!"
else
    echo "Failed to deploy Lambda."
fi
