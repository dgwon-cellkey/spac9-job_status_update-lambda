import json
import os
import traceback

import boto3

import pymysql

# DB 연결 정보 (환경 변수로 설정)
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
SQS_URL = os.getenv("SQS_URL")


# DB 연결 함수
def connect_to_DB():
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection


# Lambda 함수 시작점
def lambda_handler(event, context):
    # SQS 메시지 처리
    sqs = boto3.client("sqs")
    for record in event["Records"]:
        print(record)
        try:
            message = record["body"]
            print(f"Received message: {message}")

            # 메시지를 JSON으로 파싱
            data = json.loads(message)
            print(data)
            data = json.loads(data["Message"])
            print(data)
            data = modifi_json_for_analysis(data)
            print(data)
            receipt_handle = record["receiptHandle"]
            print(receipt_handle)

            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
            print(f"메시지 삭제 완료: {receipt_handle}")

            # 데이터베이스에 업로드
            upload_to_DB(data)

        except NameError:
            print("중복된 데이터가 감지되었습니다. 메시지를 삭제합니다.")
        except Exception as e:
            print(f"error {str(e)}")
        finally:
            try:
                receipt_handle = record["receiptHandle"]

                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                print(f"메시지 삭제 완료: {receipt_handle}")
            except Exception as e:
                print(f"delete error: {str(e)}")
                traceback.print_exc()

    return {"statusCode": 200, "body": json.dumps("Data processed successfully!")}


class DuplicateDataError(Exception):
    pass


# DB로 데이터 업로드 함수
def upload_to_DB(data):
    connection = connect_to_DB()
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT COUNT(*) as count FROM job_plan_status 
                WHERE job_plan_id = %s AND analysis_no = %s AND step = %s AND status = %s
            """
            cursor.execute(query, (data["job_plan_id"], data["analysis_no"], data["step"], data["status"]))
            result = cursor.fetchone()

            if result["count"] != 0:
                raise ValueError(f"중복된 데이터가 감지되었습니다: {data}")

            if data["status"] == "IN_PROGRESS":
                # check the completed message
                sql_select = """
                    SELECT start_date FROM job_plan_status
                    WHERE job_plan_id = %s AND step = %s AND status = 'COMPLETE'
                """
                cursor.execute(sql_select, (data["job_plan_id"], data["step"]))
                result = cursor.fetchone()

                # If a record with an COMPLETE status exists
                if result:
                    raise DuplicateDataError

                # delete wait message
                sql_delete = """
                    DELETE FROM job_plan_status
                    WHERE job_plan_id = %s AND step = %s AND status = 'WAIT'
                """
                cursor.execute(sql_delete, (data["job_plan_id"], data["step"]))

            if data["status"] == "COMPLETE":
                # Get start_date of IN_PROGRESS status with same job_plan_id, step
                sql_select = """
                    SELECT start_date FROM job_plan_status
                    WHERE job_plan_id = %s AND step = %s AND status = 'IN_PROGRESS'
                """
                cursor.execute(sql_select, (data["job_plan_id"], data["step"]))
                result = cursor.fetchone()

                # If a record with an IN_PROGRESS status exists
                if result:
                    # Replace start_date of the COMPLETE with the start_date of the IN_PROGRESS status
                    in_progress_start_date = result["start_date"]
                    data["end_date"] = data["start_date"]
                    data["start_date"] = in_progress_start_date

                # delete because of unique rule for anlaysis_no and step pair * TODO: to be reset rule into analysis_no, step, and status
                sql_delete = """
                    DELETE FROM job_plan_status
                    WHERE job_plan_id = %s AND step = %s AND status = 'IN_PROGRESS'
                """
                cursor.execute(sql_delete, (data["job_plan_id"], data["step"]))

            # SQL insert
            sql = """
                INSERT INTO job_plan_status (
                    job_plan_id, analysis_no, step, step_detail, status, 
                    description, start_date, end_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            if data["status"] == "IN_PROGRESS":
                if int(data["step"]) == 4:
                    data["description"] = "Uploading converted files to cloud"
                elif int(data["step"]) == 5:
                    data["description"] = "Processing planned job"
                elif int(data["step"]) == 6:
                    data["description"] = "Executing search analysis"
                elif int(data["step"]) == 7:
                    data["description"] = "Executing caching Files"
                elif int(data["step"]) == 8:
                    data["description"] = "Executing peptide profiling"
                elif int(data["step"]) == 9:
                    data["description"] = "Executing statistics analysis"
                elif int(data["step"]) == 10:
                    data["description"] = "Executing network analysis"
            elif data["status"] == "COMPLETE":
                if int(data["step"]) == 5:
                    data["description"] = "Completed planned job"
                elif int(data["step"]) == 6:
                    data["description"] = "Completed search analysis"
                elif int(data["step"]) == 7:
                    data["description"] = "Completed caching Files"
                elif int(data["step"]) == 8:
                    data["description"] = "Completed peptide profiling"
                elif int(data["step"]) == 9:
                    data["description"] = "Completed statistics analysis"
                elif int(data["step"]) == 10:
                    data["description"] = "Completed network analysis"
            elif data["status"] == "ERROR":
                if int(data["step"]) == 5:
                    data["description"] = "Error planned job"
                elif int(data["step"]) == 6:
                    data["description"] = "Error search analysis"
                elif int(data["step"]) == 7:
                    data["description"] = "Error caching Files"
                elif int(data["step"]) == 8:
                    data["description"] = "Error peptide profiling"
                elif int(data["step"]) == 9:
                    data["description"] = "Error statistics analysis"
                elif int(data["step"]) == 10:
                    data["description"] = "Error network analysis"

            # upload
            cursor.execute(
                sql,
                (
                    data["job_plan_id"],
                    data["analysis_no"],
                    data["step"],
                    data["step_detail"],
                    data["status"],
                    data["description"],
                    data["start_date"],
                    data["end_date"],
                ),
            )

            # add wait on next step when the step is completed
            if data["status"] == "COMPLETE" and int(data["step"]) < 10:
                next_step = str(int(data["step"]) + 1)
                cursor.execute(
                    sql,
                    (
                        data["job_plan_id"],
                        data["analysis_no"],
                        next_step,
                        data["step_detail"],
                        "WAIT",
                        f"Preparing for Step.{next_step}",
                        data["start_date"],
                        data["end_date"],
                    ),
                )

        connection.commit()
    except DuplicateDataError:
        pass
    except Exception as e:
        # 전체 스택 트레이스 출력
        traceback.print_exc()
        print(f"Error occurred: {str(e)}")
    finally:
        connection.close()


def modifi_json_for_analysis(data: dict):

    if "start_date" not in data:  # handle the step0-4
        data["start_date"] = data["timestamp"]
        data["step"] = data["step_number"]
        data["step_detail"] = data["description"]
        data["description"] = data["type"]

    data["start_date"] = timestamp_modi(data["start_date"])
    data["end_date"] = None

    if "analysis_no" not in data:
        splitted_job_plan_id = data["job_plan_id"].split("_")
        if len(splitted_job_plan_id) == 3:
            data["analysis_no"] = splitted_job_plan_id[1]

    if "status" not in data:
        status = ""
        if data["description"].lower().startswith("start"):
            status = "IN_PROGRESS"
        elif data["description"].lower().startswith("finish"):
            status = "COMPLETE"
        elif data["description"].lower().startswith("error"):
            status = "ERROR"

        data["status"] = status

    if data["step_detail"]:
        data["description"] = data["step_detail"]
    else:
        step_detail = ""
        step = data["step"]
        if step == 8:
            step_detail = "SEARCHED PROCESS"
        elif step == 9:
            step_detail = "STATISTICS PROCESS"
        elif step == 10:
            step_detail = "NETWORK PROCESS"
        data["step_detail"] = step_detail

    return data


def timestamp_modi(timestamp_str):
    if "," in timestamp_str:
        timestamp_str = timestamp_str.replace(",", ".")

    return timestamp_str


if __name__ == "__main__":
    test_json = '{"job_plan_id": 36, "analysis_no": "dev_test", "step": 9, "step_detail": "", "description": "Finish searched process", "start_date": "2024-09-30 06:46:18,328", "end_date": "2024-09-30 06:46:18,328"}'
    test_json = json.loads(test_json)

    print(test_json)

    modifi_json_for_analysis(test_json)

    upload_to_DB(test_json)
