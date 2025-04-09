from flask import Flask, request, jsonify
import requests
import json
import os

app = Flask(__name__)

# CORS 허용
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook 광고 성과 보고서 API가 실행 중입니다."})

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
        
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({"error": "시작 날짜와 종료 날짜를 모두 입력해주세요."}), 400
        
        # 환경 변수에서 Facebook 계정 ID와 액세스 토큰 가져오기
        ver = "v19.0"
        account = os.environ.get("FACEBOOK_ACCOUNT_ID")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN")

        if not account or not token:
            print("Error: Facebook Account ID or Access Token not found in environment variables.")
            # 기본값을 사용하지 않고 오류를 반환하거나, 로깅 후 기본값 사용 결정
            # 여기서는 일단 오류로 처리하는 것이 안전합니다.
            return jsonify({"error": "Server configuration error: Missing Facebook credentials."}), 500

        print(f"Attempting to fetch data for account: {account} from {start_date} to {end_date}") # 로깅 추가

        # Facebook 광고 데이터 가져오기
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        print("Successfully fetched and formatted data.") # 성공 로깅 추가
        return jsonify(result)

    except requests.exceptions.RequestException as req_err: # requests 관련 오류 명시적 처리
        print(f"Error during Facebook API request: {str(req_err)}")
        return jsonify({"error": f"API request failed: {str(req_err)}"}), 500
    except KeyError as key_err: # API 응답 파싱 오류 등
        print(f"Error processing API response (KeyError): {str(key_err)}")
        return jsonify({"error": f"Error processing API data: {str(key_err)}"}), 500
    except Exception as e:
        # traceback 모듈을 사용하여 더 자세한 오류 정보 로깅
        import traceback
        error_details = traceback.format_exc()
        print(f"An unexpected error occurred: {str(e)}\nDetails:\n{error_details}")
        # 사용자에게는 간단한 메시지 전달
        return jsonify({"error": "An internal server error occurred while generating the report."}), 500
