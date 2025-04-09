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

@app.route('/api/generate-report', methods=['POST', 'OPTIONS'])
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
        account = os.environ.get("FACEBOOK_ACCOUNT_ID", "act_1601012230605073")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN", "EAANZBwKbZBHwsBO1i1lgkwYZAWeaASGa46fvoDZBxSwXuUZCjgGuz1yT0Vcry7ANUdpAyJoGLeGdEhRvcSsFBkVFKWZB6bfzksEJ5z9vgjQ3L6Vb7Ax0e36U9FMk7YGYBfU5TqDmG0hjpm7WyqVDjN3u55TGRxcvO0IgOgPaApa05GhzmXsondFBpnurK1brIbCqwhqi5E")
        
        # Facebook 광고 데이터 가져오기
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        return jsonify(result)
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({"error": str(e)}), 500
