import json
import traceback

import utils


class DuplicateDataError(Exception):
    pass


def pre_insert_stage(data: dict, records: list, cursor) -> dict:
    """
    INSERT 전 단계:
      - 동일 analysis_no, step, status 레코드가 있으면 중복으로 판단
      - IN_PROGRESS 상태의 경우 동일 step의 COMPLETE 레코드 존재 여부 확인 후 WAIT 레코드 삭제
      - COMPLETE/ERROR 상태의 경우 IN_PROGRESS 또는 WAIT 레코드가 있다면 start_date를 갱신 후 삭제
    """
    # 중복 레코드 체크
    for rec in records:
        if (
            rec["analysis_no"] == data["analysis_no"]
            and str(rec["step"]) == str(data["step"])
            and rec["status"] == data["status"]
        ):
            raise ValueError(f"Duplicate data detected: {data}")

    # 1 Step당 1개의 Status 메세지만 남기기
    # IN_PROGRESS의 경우 -> WAIT 삭제
    if data["status"] == "IN_PROGRESS":
        for rec in records:
            if str(rec["step"]) == str(data["step"]) and rec["status"] == "COMPLETE":
                raise DuplicateDataError("Complete record already exists for this step")
        delete_query = "DELETE FROM job_plan_status WHERE job_plan_id = %s AND step = %s AND status = 'WAIT'"
        cursor.execute(delete_query, (data["job_plan_id"], data["step"]))
    # COMPLETE or ERROR의 경우 -> IN_PROGRESS나 WAIT 삭제
    elif data["status"] in ("COMPLETE", "ERROR"):
        for rec in records:
            if str(rec["step"]) == str(data["step"]) and rec["status"] in ("IN_PROGRESS", "WAIT"):
                # IN_PROGRESS/WAIT 레코드의 start_date를 사용
                in_progress_start_date = rec["start_date"]
                data["end_date"] = data["start_date"]
                data["start_date"] = in_progress_start_date
                break
        delete_query = (
            "DELETE FROM job_plan_status " "WHERE job_plan_id = %s AND step = %s AND status IN ('IN_PROGRESS', 'WAIT')"
        )
        cursor.execute(delete_query, (data["job_plan_id"], data["step"]))
    return data


def perform_insert(data: dict, cursor):
    """
    INSERT 실행 단계:
      - 주어진 데이터를 job_plan_status 테이블에 삽입합니다.
    """
    insert_query = """
        INSERT INTO job_plan_status (
            job_plan_id, analysis_no, step, step_detail, status, 
            description, start_date, end_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(
        insert_query,
        (
            data["job_plan_id"],
            data["analysis_no"],
            data["step"],
            data.get("step_detail", ""),
            data["status"],
            data["description"],
            data["start_date"],
            data["end_date"],
        ),
    )


def post_insert_stage(data: dict, cursor):
    """
    INSERT 후 단계:
      - COMPLETE 상태이고 step이 10 미만이면 다음 step의 WAIT 메시지를 추가합니다.
    """
    if data["status"] == "COMPLETE" and int(data["step"]) < 10:
        next_step = str(int(data["step"]) + 1)
        wait_data = data.copy()
        wait_data["step"] = next_step
        wait_data["status"] = "WAIT"
        wait_data["description"] = f"Preparing for Step.{next_step}"

        insert_query = """
            INSERT INTO job_plan_status (
                job_plan_id, analysis_no, step, step_detail, status, 
                description, start_date, end_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            insert_query,
            (
                wait_data["job_plan_id"],
                wait_data["analysis_no"],
                wait_data["step"],
                wait_data.get("step_detail", ""),
                wait_data["status"],
                wait_data["description"],
                wait_data["start_date"],
                wait_data["end_date"],
            ),
        )


def upload_to_DB(data: dict, secrets: dict):
    """
    DB 업로드 전체 흐름:
      1. job_plan_id에 해당하는 전체 레코드를 단일 Query로 조회
      2. pre_insert_stage()를 통해 조건 검사 및 기존 레코드 삭제
      3. perform_insert()로 데이터 삽입
      4. post_insert_stage()를 통해 COMPLETE 상태일 경우 다음 step의 WAIT 메시지 추가
    """
    connection = utils.connect_to_DB(secrets)
    try:
        with connection.cursor() as cursor:
            query = "SELECT * FROM job_plan_status WHERE job_plan_id = %s"
            cursor.execute(query, (data["job_plan_id"],))
            records = cursor.fetchall()

            data = pre_insert_stage(data, records, cursor)  # Start/End date 갱신되는 경우를 위한 업데이트
            connection.commit()
            perform_insert(data, cursor)
            connection.commit()
            post_insert_stage(data, cursor)
            connection.commit()
    except DuplicateDataError:
        print("DuplicateDataError 발생. DB 업로드를 건너뜁니다.")
    except Exception as e:
        traceback.print_exc()
        print(f"Error occurred: {str(e)}")
    finally:
        connection.close()


def lambda_handler(event, context):
    """
    Lambda 함수 진입점:
    1. SQS 메시지를 순차적으로 처리
    2. 메시지 본문을 2단계 JSON 파싱 후 전처리 수행
    3. SQS에서 메시지를 삭제하고 DB에 업로드
    """
    secrets = utils.get_secrets()
    for record in event["Records"]:
        try:
            body = json.loads(record["body"])
            message = json.loads(body["Message"])
            print(message)
            data = utils.modifi_message_for_analysis(message)
            upload_to_DB(data, secrets)
        except Exception as e:
            print(f"Error processing record: {e}")
            traceback.print_exc()
            # 에러 발생 시 SQS 메시지 삭제
            receipt_handle = record["receiptHandle"]
            try:
                utils.delete_sqs_message(receipt_handle, secrets["sqs_url"])
                print(f"SQS 메시지 삭제 완료 (에러 발생 후): {receipt_handle}")
            except Exception as delete_err:
                print(f"SQS 메시지 삭제 실패 (에러 발생 후): {delete_err}")
        else:
            # 작업이 정상적으로 완료되었을 경우 SQS 메시지 삭제
            receipt_handle = record["receiptHandle"]
            try:
                utils.delete_sqs_message(receipt_handle, secrets["sqs_url"])
                print(f"SQS 메시지 삭제 완료 (정상 완료): {receipt_handle}")
            except Exception as delete_err:
                print(f"SQS 메시지 삭제 실패 (정상 완료): {delete_err}")

    return {"statusCode": 200, "body": json.dumps("Data processed successfully!")}


if __name__ == "__main__":
    test_json = '{"job_plan_id": 36, "analysis_no": "dev_test", "step": 9, "step_detail": "", "description": "Finish searched process", "start_date": "2024-09-30 06:46:18,328", "end_date": "2024-09-30 06:46:18,328"}'
    test_json = json.loads(test_json)

    print(test_json)

    utils.modifi_message_for_analysis(test_json)

    upload_to_DB(test_json)
