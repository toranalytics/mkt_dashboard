from flask import Flask, request, jsonify
import requests
import json
import os
import traceback
import pandas as pd
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

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
        
        # 패스워드 보호: POST 데이터에 'password' 필드가 있고, 환경 변수 REPORT_PASSWORD와 일치해야 함.
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 시작/종료 날짜 기본값 설정: 오늘의 전날 (YYYY-MM-DD)
        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date

        ver = "v19.0"
        account = os.environ.get("FACEBOOK_ACCOUNT_ID")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN")
        if not account or not token:
            print("Error: Facebook Account ID or Access Token not found in environment variables.")
            return jsonify({"error": "Server configuration error: Missing Facebook credentials."}), 500

        print(f"Attempting to fetch data for account: {account} from {start_date} to {end_date}")
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        print("Successfully fetched and formatted data.")
        return jsonify(result)

    except requests.exceptions.RequestException as req_err:
        print(f"Error during Facebook API request: {str(req_err)}")
        return jsonify({"error": f"API request failed: {str(req_err)}"}), 500
    except KeyError as key_err:
        print(f"Error processing API response (KeyError): {str(key_err)}")
        return jsonify({"error": f"Error processing API data: {str(key_err)}"}), 500
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"An unexpected error occurred: {str(e)}\nDetails:\n{error_details}")
        return jsonify({"error": "An internal server error occurred while generating the report."}), 500


# --------------------------------------------------------------------------------
# 광고 크리에이티브 미디어 URL 병렬 처리 (이미지 및 비디오)
# --------------------------------------------------------------------------------

def get_creative_media_urls(ad_id, ver, token):
    creative_url = f"https://graph.facebook.com/{ver}/{ad_id}"
    creative_params = {
        'fields': 'creative',
        'access_token': token
    }
    creative_response = requests.get(url=creative_url, params=creative_params)
    result = {"image_url": "", "video_url": ""}
    
    if creative_response.status_code == 200:
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')
        if creative_id:
            media_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            media_params = {
                'fields': 'image_url,thumbnail_url,object_story_spec,video_id',
                'access_token': token
            }
            media_response = requests.get(url=media_req_url, params=media_params)
            if media_response.status_code == 200:
                media_data = media_response.json()
                
                # 비디오 ID가 있는 경우 비디오 URL 가져오기
                video_id = media_data.get('video_id')
                if video_id:
                    video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
                    video_params = {
                        'fields': 'source',
                        'access_token': token
                    }
                    video_response = requests.get(url=video_req_url, params=video_params)
                    if video_response.status_code == 200:
                        video_data = video_response.json()
                        result["video_url"] = video_data.get('source', '')
                
                # 이미지 URL 처리 (기존 로직)
                image_url = media_data.get('image_url')
                if not image_url and 'object_story_spec' in media_data:
                    story_spec = media_data.get('object_story_spec', {})
                    if 'photo_data' in story_spec:
                        image_url = story_spec.get('photo_data', {}).get('image_url')
                    elif 'link_data' in story_spec and 'image_url' in story_spec.get('link_data', {}):
                        image_url = story_spec.get('link_data', {}).get('image_url')
                if not image_url:
                    image_url = media_data.get('thumbnail_url')
                
                result["image_url"] = image_url
    
    return result

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for ad_id in ad_data.keys():
            futures[executor.submit(get_creative_media_urls, ad_id, ver, token)] = ad_id
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                media_urls = future.result()
                ad_data[ad_id]['image_url'] = media_urls.get("image_url", "")
                ad_data[ad_id]['video_url'] = media_urls.get("video_url", "")
            except Exception:
                ad_data[ad_id]['image_url'] = ""
                ad_data[ad_id]['video_url'] = ""

# --------------------------------------------------------------------------------
# 메인 함수: 구매 수와 구매당 비용 및 광고 성과 (구매당 비용 기준) 추가
# --------------------------------------------------------------------------------

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }
    response = requests.get(url=insights_url, params=params)
    if response.status_code != 200:
        raise Exception(f"성과 데이터 불러오기 오류: {response.text}")
    data = response.json()
    records = data.get('data', [])
    ad_data = {}

    # 1) actions에서 구매 수 (purchase_count) 및 링크 클릭(link_clicks) 추출
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue
        
        link_clicks = 0
        purchase_count = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "link_click":
                    try:
                        link_clicks += int(action.get("value", 0))
                    except ValueError:
                        link_clicks += 0
                # purchase: 필요 시 여러 purchase 유형(omni_purchase 등) 추가
                if action.get("action_type") == "purchase":
                     try:
                        purchase_count += int(action.get("value", 0))
                    except ValueError:
                        purchase_count += 0
