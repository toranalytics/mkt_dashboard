from flask import Flask, request, jsonify
import requests
import json
import os
# traceback 모듈은 except 블록 안에서 import 해도 괜찮지만, 
# 파일 상단에 import하는 것이 더 일반적일 수 있습니다. (선택 사항)
import traceback 

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
        # Preflight 요청 처리
        return jsonify({}), 200
        
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({"error": "시작 날짜와 종료 날짜를 모두 입력해주세요."}), 400
        
        # 환경 변수에서 Facebook 계정 ID와 액세스 토큰 가져오기
        ver = "v19.0" # Facebook API 버전
        account = os.environ.get("FACEBOOK_ACCOUNT_ID")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN")

        if not account or not token:
            print("Error: Facebook Account ID or Access Token not found in environment variables.")
            # 환경 변수 누락 시 서버 설정 오류 반환
            return jsonify({"error": "Server configuration error: Missing Facebook credentials."}), 500

        print(f"Attempting to fetch data for account: {account} from {start_date} to {end_date}") # 로깅 추가

        # ================================================================
        # !!! 오류 발생 가능성이 높은 지점 !!!
        # fetch_and_format_facebook_ads_data 함수가 정의되어 있어야 하고,
        # 이 함수 내부에서 오류가 발생하지 않아야 합니다.
        # 필요한 라이브러리(예: facebook_business)가 requirements.txt에 있어야 합니다.
        # ================================================================
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token) 
        
        print("Successfully fetched and formatted data.") # 성공 로깅 추가
        return jsonify(result)

    except requests.exceptions.RequestException as req_err: # requests 라이브러리 관련 네트워크 오류 등 명시적 처리
        print(f"Error during Facebook API request: {str(req_err)}")
        return jsonify({"error": f"API request failed: {str(req_err)}"}), 500
    
    except KeyError as key_err: # Facebook API 응답 파싱 중 키 오류 등
        print(f"Error processing API response (KeyError): {str(key_err)}")
        return jsonify({"error": f"Error processing API data: {str(key_err)}"}), 500
    
    except Exception as e: # 그 외 예상치 못한 모든 오류 처리
        # traceback 모듈을 사용하여 더 자세한 오류 정보 로깅
        error_details = traceback.format_exc()
        print(f"An unexpected error occurred: {str(e)}\nDetails:\n{error_details}")
        # 사용자에게는 간단한 내부 서버 오류 메시지 전달
        return jsonify({"error": "An internal server error occurred while generating the report."}), 500

# ================================================================
# !!! 중요 !!!
# 아래 함수는 실제 구현이 필요합니다. 이 파일 또는 다른 파일에 정의되어야 합니다.
# 예시 이름이며, 실제 프로젝트의 함수 이름과 일치해야 합니다.
# ================================================================
# def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
#     # 여기에 Facebook Marketing API를 호출하고 데이터를 처리하는 로직 구현
#     # 예: facebook_business SDK 사용 또는 requests 라이브러리로 직접 API 호출
#     # ... API 호출 ...
#     # ... 데이터 파싱 및 포맷팅 ...
#     # dummy_data = [{"campaign_name": "Test Campaign", "spend": 100, "clicks": 10}] 
#     # return dummy_data # 처리된 데이터 반환
#     pass # 실제 구현 필요
# ================================================================

# Flask 앱 실행 (로컬 테스트용, Vercel에서는 필요 없음)
# if __name__ == '__main__':
#    app.run(debug=True)
