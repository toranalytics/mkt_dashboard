from flask import Flask, request, jsonify
import requests
import json
import pandas as pd
import os

app = Flask(__name__)

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook 광고 성과 보고서 API가 실행 중입니다."})

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
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
        return jsonify({"error": str(e)}), 500

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc'
    url = f"https://graph.facebook.com/{ver}/{account}/insights"

    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }

    response = requests.get(url=url, params=params)
    ad_data = {}

    if response.status_code != 200:
        return {"error": f"성과 데이터 불러오기 오
