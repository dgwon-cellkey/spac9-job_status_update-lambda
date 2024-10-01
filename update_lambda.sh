#!/bin/bash

# 변수 정의
FUNCTION_NAME="spac9-job_plan_status_update"  # Lambda 함수 이름
ZIP_FILE="lambda_package.zip"              # 생성할 ZIP 파일 이름
SRC_DIR="."                   # 코드와 패키지가 있는 디렉터리

mkdir "lambda_package"
cd lambda_package
cp ../lambda_function.py ./

# 1. 패키지 설치 (필요한 경우만)
# pymysql 등 필요한 패키지를 lambda_package 디렉터리에 설치
echo "Installing dependencies..."
pip install pymysql -t $SRC_DIR

# 2. ZIP 파일 생성
echo "Creating zip file..."
cd $SRC_DIR
zip -r ../$ZIP_FILE .  # lambda_package 디렉터리 내부를 압축
cd ..

# 3. AWS Lambda에 코드 업로드
echo "Updating Lambda function..."
aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://$ZIP_FILE

# 4. 결과 확인
if [ $? -eq 0 ]; then
    echo "Lambda function updated successfully!"
else
    echo "Failed to update Lambda function."
fi
