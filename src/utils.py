import json
import os

import boto3
import pymysql

# description 매핑 정보, 실제 고객에게 나가는 메세지
DESCRIPTION_MAPPING = {
    "IN_PROGRESS": {
        4: "Uploading converted files to cloud",
        5: "Processing planned job",
        6: "Executing search analysis",
        7: "Executing caching files",
        8: "Executing peptide profiling",
        9: "Executing statistics analysis",
        10: "Executing network analysis",
    },
    "COMPLETE": {
        5: "Completed planned job",
        6: "Completed search analysis",
        7: "Completed caching files",
        8: "Completed peptide profiling",
        9: "Completed statistics analysis",
        10: "Completed network analysis",
    },
    "ERROR": {
        5: "Error planned job",
        6: "Error search analysis",
        7: "Error caching files",
        8: "Error peptide profiling",
        9: "Error statistics analysis",
        10: "Error network analysis",
    },
}


def modifi_message_for_analysis(data: dict) -> dict:
    """
    메시지 데이터 전처리:
    - step0~4에 해당하면 별도 함수를 호출
        Step0-4 예시 {'filename': None, 'group_id': None, 'sequence_id': '1/1', 'folder_name': '20250314_MSQWER25007_001', \
            'job_plan_id': 2070, 'analysis_no': 'MSQWER25007', 'step': 1, 'description': 'start', \
            'step_detail': 'Monitoring RAW file creation', 'start_date': '2025-03-27 05:40:51'}
    - timestamp 정규화
    - description 내용을 바탕으로 status 결정 (start → IN_PROGRESS, finish → COMPLETE, error → ERROR)
    - step_detail이 비어있으면 step 번호에 따라 기본값 할당
    """
    if "start_date" not in data:
        data = process_step0_4(data)

    data["start_date"] = timestamp_modi(data["start_date"])
    data["end_date"] = None

    # status 결정: 이미 존재하지 않으면 description을 기준으로 설정
    if not data.get("status", ""):
        desc_lower = data.get("description", "").lower()
        if desc_lower.startswith("start"):
            data["status"] = "IN_PROGRESS"
        elif desc_lower.startswith("finish"):
            data["status"] = "COMPLETE"
        elif desc_lower.startswith("error"):
            data["status"] = "ERROR"
        else:
            data["status"] = ""

    # description 매핑 갱신 for user
    data["description"] = get_description(data["status"], data["step"], data.get("description", ""))

    return data


def get_secrets():
    secret_name = "config/spac9-analysis"
    region_name = "ap-northeast-2"

    # Secrets Manager 클라이언트 생성
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f"Secrets Manager에서 비밀을 가져오는 중 에러 발생: {e}")
        raise e

    secret_string = get_secret_value_response.get("SecretString")
    if not secret_string:
        raise ValueError("Secrets Manager에서 SecretString이 반환되지 않았습니다.")

    secret = json.loads(secret_string)
    return secret


def connect_to_DB(secrets: dict):
    host = secrets.get("db_host")
    user = secrets.get("db_username")
    password = secrets.get("db_password")
    database = secrets.get("db_dbname")

    if not all([host, user, password, database]):
        raise ValueError("Secrets Manager에 저장된 DB 연결 정보가 불완전합니다.")

    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def delete_sqs_message(receipt_handle, sqs_url):
    sqs = boto3.client("sqs")
    sqs.delete_message(QueueUrl=sqs_url, ReceiptHandle=receipt_handle)


def get_description(status, step, default_desc=""):
    """
    주어진 상태와 step에 따른 description을 리턴합니다.
    """
    return DESCRIPTION_MAPPING[status][int(step)]


def process_step0_4(data: dict) -> dict:
    """
    step이 0~4에 해당하는 경우의 데이터 전처리. # TODO: Tornike 코드에서 수정하는게 더 이상적
    'timestamp', 'step_number', 'description', 'type' 키를 사용하여
    start_date, step, step_detail, description을 채웁니다.
    """
    data["start_date"] = data.get("timestamp")
    data["step"] = data.get("step_number")
    data["step_detail"] = data.get("description")
    data["description"] = data.get("type")

    return data


def timestamp_modi(timestamp_str: str) -> str:
    """
    timestamp 문자열의 ',' 구분자를 '.'로 변경하여 일관성을 유지합니다.
    """
    return timestamp_str.replace(",", ".") if "," in timestamp_str else timestamp_str
