# api/index.py
# -*- coding: utf-8 -*-
import math # categorize_performance 에서 math.isinf 사용 위해 추가
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
import json

import pandas as pd
import requests
from flask import Flask, jsonify, request
import re

# --- Cafe24 API 모듈 import ---
# (기존 코드 유지)
try:
    from . import cafe24_api
    CAFE24_CONFIGS = cafe24_api.CAFE24_CONFIGS
    process_cafe24_data = cafe24_api.process_cafe24_data
    print("Successfully imported Cafe24 module.")
except ImportError as e:
    print(f"CRITICAL: Could not import cafe24_api module: {e}. Cafe24 features will be disabled.")
    traceback.print_exc()
    CAFE24_CONFIGS = {}
    def process_cafe24_data(*args, **kwargs):
        print("Executing dummy process_cafe24_data due to import error.")
        return {"visitors": {}, "sales": {}}

# .env 파일 로드
# (기존 코드 유지)
try:
    from dotenv import load_dotenv
    # .env 파일 경로 탐색 (현재 폴더 -> 상위 폴더 순)
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    if not os.path.exists(dotenv_path):
        dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if not os.path.exists(dotenv_path):
        dotenv_path = '.env' # 최후의 수단: 실행 위치 기준 .env

    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        print(f"dotenv loaded from {os.path.abspath(dotenv_path)}.")
    else:
        print(f"dotenv file not found in {os.path.dirname(__file__)} or parent directory, skipping .env load.")
except ImportError:
    print("dotenv not installed, skipping .env load.")
    pass

# --- Flask 앱 인스턴스 생성 (Vercel 필수) ---
app = Flask(__name__)

# --- Meta 계정 설정 로드 ---
# (기존 코드 유지)
def load_account_configs():
    accounts = {}
    i = 1
    while True:
        name = os.environ.get(f"ACCOUNT_CONFIG_{i}_NAME")
        account_id = os.environ.get(f"ACCOUNT_CONFIG_{i}_ID")
        token = os.environ.get(f"ACCOUNT_CONFIG_{i}_TOKEN")
        if name and account_id and token:
            # 공백 제거
            name = name.strip()
            account_id = account_id.strip()
            token = token.strip()
            if name and account_id and token: # 제거 후 다시 확인
                accounts[name] = {"id": account_id, "token": token, "name": name}
                print(f"Loaded Meta account: {name} (ID: {account_id})")
                i += 1
            else:
                print(f"Warning: Skipped Meta account config index {i} due to empty value after stripping.")
                i += 1 # 다음 인덱스로 이동 시도
        else:
            # 설정 하나라도 있으면 경고 (설정 누락 가능성)
            if name or account_id or token:
                print(f"Warning: Incomplete Meta account config index {i}. Check name, id, token.")
            # 설정이 아예 없으면 루프 종료
            break
    if not accounts:
        print("CRITICAL: No complete Meta account configurations found in environment variables (e.g., ACCOUNT_CONFIG_1_NAME, ACCOUNT_CONFIG_1_ID, ACCOUNT_CONFIG_1_TOKEN).")
    return accounts
ACCOUNT_CONFIGS = load_account_configs()

# --- Cafe24 계정 설정 로드 (Meta 계정과 매핑되도록) ---
# (기존 코드 유지)
def load_cafe24_configs_from_env():
    configs = {}
    i = 1
    while True:
        # Meta 계정 이름과 동일한 키를 사용하는지 확인 (예: ACCOUNT_CONFIG_1_NAME)
        meta_account_name = os.environ.get(f"ACCOUNT_CONFIG_{i}_NAME")
        if not meta_account_name:
            break # 더 이상 Meta 계정 설정이 없으면 종료

        # 해당 Meta 계정에 대한 Cafe24 설정 찾기
        mall_id = os.environ.get(f"CAFE24_CONFIG_{i}_MALL_ID")
        client_id = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_ID")
        client_secret = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_SECRET")
        access_token = os.environ.get(f"CAFE24_CONFIG_{i}_ACCESS_TOKEN")
        refresh_token = os.environ.get(f"CAFE24_CONFIG_{i}_REFRESH_TOKEN")

        # 필수 정보가 모두 있는지 확인
        if mall_id and client_id and client_secret and access_token and refresh_token:
             # 공백 제거
             meta_account_name = meta_account_name.strip()
             mall_id = mall_id.strip()
             client_id = client_id.strip()
             client_secret = client_secret.strip()
             access_token = access_token.strip()
             refresh_token = refresh_token.strip()

             if meta_account_name and mall_id and client_id and client_secret and access_token and refresh_token: # 재확인
                 configs[meta_account_name] = {
                     "mall_id": mall_id,
                     "client_id": client_id,
                     "client_secret": client_secret,
                     "access_token": access_token,
                     "refresh_token": refresh_token,
                     "token_file": f"cafe24_token_{meta_account_name.replace(' ', '_').lower()}.json" # 고유한 토큰 파일명
                 }
                 print(f"Loaded Cafe24 config for Meta account: {meta_account_name} (Mall ID: {mall_id})")
             else:
                 print(f"Warning: Skipped Cafe24 config for Meta account index {i} due to empty value after stripping.")

        elif mall_id or client_id or client_secret or access_token or refresh_token:
             # 일부 정보만 있는 경우 경고
             print(f"Warning: Incomplete Cafe24 config for Meta account index {i} ('{meta_account_name}'). Missing some required fields (MALL_ID, CLIENT_ID, CLIENT_SECRET, ACCESS_TOKEN, REFRESH_TOKEN).")

        i += 1 # 다음 인덱스로

    if not configs:
        print("Notice: No complete Cafe24 configurations found in environment variables (e.g., CAFE24_CONFIG_1_MALL_ID, ...). Cafe24 data integration will be limited.")
    return configs

# cafe24_api 모듈이 성공적으로 임포트되었는지 확인 후 설정 로드
if 'cafe24_api' in globals():
    # 환경변수에서 Cafe24 설정 로드 시도
    CAFE24_CONFIGS = load_cafe24_configs_from_env()
    # cafe24_api 모듈에도 로드된 설정 전달 (필요한 경우)
    if hasattr(cafe24_api, 'set_configs'):
        cafe24_api.set_configs(CAFE24_CONFIGS)
    elif not CAFE24_CONFIGS:
        # 환경 변수 로드 실패했고, cafe24_api.CAFE24_CONFIGS 도 비어있다면 경고
        if not getattr(cafe24_api, 'CAFE24_CONFIGS', {}):
             print("Notice: Cafe24 configs not found in environment variables nor hardcoded in cafe24_api module.")
else:
     print("Warning: Cafe24 module not loaded, skipping Cafe24 config loading from environment.")
     CAFE24_CONFIGS = {} # 모듈 로드 실패 시 빈 딕셔너리로 설정

# CORS 허용 설정
# (기존 코드 유지)
@app.after_request
def after_request(response):
    # 모든 Origin 허용 (*) 또는 특정 Origin 지정 가능
    # origin = request.headers.get('Origin')
    allowed_origins = ['*'] # 필요시 특정 도메인 리스트로 변경: ['https://your-frontend.com', 'http://localhost:3000']

    # 와일드카드 사용 시 request origin 사용 불필요
    response.headers.add('Access-Control-Allow-Origin', '*')
    # if origin in allowed_origins or '*' in allowed_origins:
    #     response.headers.add('Access-Control-Allow-Origin', origin if origin else '*')

    # 허용할 헤더 및 메소드 설정
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS') # OPTIONS 포함 중요
    response.headers.add('Access-Control-Allow-Credentials', 'true') # 필요 시 추가 (인증 정보 포함 시)
    return response

# 기본 경로 및 /api 경로 핸들러
# (기존 코드 유지)
@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook & Cafe24 Ads Report API is running."})

# 계정 목록 반환 API
# (기존 코드 유지)
@app.route('/api/accounts', methods=['POST', 'OPTIONS'])
def get_accounts():
    if request.method == 'OPTIONS': # Preflight 요청 처리
        return jsonify({}), 200
    try:
        # 비밀번호 검증
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required."}), 400
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")

        # REPORT_PASSWORD 환경 변수가 설정되어 있고, 비밀번호가 틀리면 403 반환
        if report_password and (not password or password != report_password):
            print(f"Access denied for get_accounts: Incorrect password attempt.") # 로그 추가
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 로드된 Meta 계정 이름 목록 반환
        account_names = list(ACCOUNT_CONFIGS.keys())
        if not account_names:
             print("Warning: No Meta accounts configured, returning empty list.")
        return jsonify(account_names)

    except Exception as e:
        print(f"Error getting account list: {e}")
        traceback.print_exc()
        return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API ---
@app.route('/api/generate-report', methods=['POST', 'OPTIONS'])
def generate_report():
    if request.method == 'OPTIONS': # Preflight 요청 처리
        return jsonify({}), 200
    try:
        # 1. 요청 데이터 파싱 및 검증
        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required."}), 400

        # 비밀번호 검증
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
            print(f"Access denied for generate_report: Incorrect password attempt.") # 로그 추가
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 날짜 파싱 (기본값: 어제)
        today = datetime.now().date()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date_str = data.get('start_date') or default_date
        end_date_str = data.get('end_date') or start_date_str
        try:
            datetime.strptime(start_date_str, '%Y-%m-%d')
            datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        print(f"Report requested for date range: {start_date_str} to {end_date_str}")

        # 대상 Meta 계정 선택
        selected_account_key = data.get('selected_account_key')
        if not ACCOUNT_CONFIGS:
             return jsonify({"error": "설정된 Meta 광고 계정이 없습니다. 환경 변수를 확인하세요."}), 500

        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1:
                selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
                print(f"Only one Meta account found, automatically selected: '{selected_account_key}'")
            else:
                return jsonify({"error": f"Meta 계정을 선택해주세요. 사용 가능: {', '.join(ACCOUNT_CONFIGS.keys())}"}), 400

        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config:
            return jsonify({"error": f"선택한 Meta 계정 키 '{selected_account_key}'에 대한 설정을 찾을 수 없습니다."}), 404

        meta_account_id = account_config.get('id')
        meta_token = account_config.get('token')
        if not meta_account_id or not meta_token:
             print(f"Error: Incomplete config for Meta account '{selected_account_key}'. Missing ID or Token.")
             return jsonify({"error": f"Meta 계정 '{selected_account_key}'의 설정(ID 또는 토큰)이 올바르지 않습니다."}), 500

        # Meta API 버전 (최신 버전 사용 권장)
        meta_api_version = "v19.0" # 필요시 업데이트
        print(f"Using Meta Account: '{selected_account_key}' (ID: {meta_account_id}), API Version: {meta_api_version}")

        # --- 2. Cafe24 총계 데이터 가져오기 ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0}
        daily_cafe24_data = {"visitors": {}, "sales": {}}

        # Cafe24 API 모듈 로드 성공 및 해당 Meta 계정에 대한 Cafe24 설정이 있는지 확인
        if 'cafe24_api' in globals() and CAFE24_CONFIGS and selected_account_key in CAFE24_CONFIGS:
            matching_cafe24_config = CAFE24_CONFIGS[selected_account_key]
            print(f"Found matching Cafe24 config for '{selected_account_key}'. Attempting to fetch Cafe24 data...")
            try:
                # cafe24_api.process_cafe24_data 함수 호출
                daily_cafe24_data = process_cafe24_data(
                    selected_account_key, # 현재 처리 중인 Meta 계정 키 전달
                    matching_cafe24_config,
                    start_date_str,
                    end_date_str
                )

                # 반환된 데이터 구조 확인 및 총계 계산
                if daily_cafe24_data and isinstance(daily_cafe24_data.get("visitors"), dict) and isinstance(daily_cafe24_data.get("sales"), dict):
                    total_visitors = sum(daily_cafe24_data["visitors"].values())
                    total_sales = sum(daily_cafe24_data["sales"].values())
                    cafe24_totals = {"total_visitors": int(round(total_visitors)), "total_sales": int(round(total_sales))}
                    print(f"Cafe24 totals calculated. Visitors: {cafe24_totals['total_visitors']}, Sales: {cafe24_totals['total_sales']}")
                else:
                    print("Warning: process_cafe24_data did not return the expected structure (dict with 'visitors' and 'sales' keys). Skipping Cafe24 totals.")
            except Exception as cafe24_err:
                print(f"Error during Cafe24 processing for '{selected_account_key}': {cafe24_err}")
                traceback.print_exc()
                # Cafe24 오류 발생 시에도 Meta 데이터 처리는 계속 진행
        elif 'cafe24_api' in globals() and selected_account_key not in CAFE24_CONFIGS:
             print(f"Notice: No matching Cafe24 config found for Meta account '{selected_account_key}'. Skipping Cafe24 data fetch.")
        else:
            # Cafe24 모듈 자체가 로드되지 않은 경우
            print("Notice: Cafe24 module or configs not loaded. Skipping Cafe24 totals.")

        # --- 3. Meta 광고 데이터 가져오기 및 최종 보고서 생성 ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        # ★★★ 수정된 fetch_and_format_facebook_ads_data 함수 사용 ★★★
        final_report_data = fetch_and_format_facebook_ads_data(
            start_date_str, end_date_str, meta_api_version, meta_account_id, meta_token,
            cafe24_totals # 계산된 Cafe24 총계 전달
        )
        print("Meta Ads data fetch and report formatting completed.")

        # --- 4. 결과 반환 ---
        print("--- Report generation process complete ---")
        # 최종 데이터에는 html_table과 data(JSON용)가 포함됨
        # cafe24_totals는 html_table 생성 시 사용되었으므로 별도 반환 필요 없음
        return jsonify(final_report_data)

    except Exception as e:
        error_message = "An internal server error occurred during report generation."
        print(f"{error_message} Details: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": error_message, "details": str(e)}), 500


def get_creative_details(ad_id, ver, token):
    """광고 ID를 사용하여 크리에이티브 상세 정보를 가져옵니다. (특정 ID 오류 로깅 강화)"""
    # 기본 반환 구조
    creative_details = {
        'content_type': '알 수 없음', 'display_url': '',
        'target_url': '', 'creative_asset_url': ''
    }
    creative_id = None
    details_data = None # API 호출 성공 여부 확인용
    # ★★★ 문제의 Creative ID ★★★
    DEBUG_CREATIVE_ID = "1348468862968678" # '러닝 대표님 이미지' ID (이전 제공 기준)

    try:
        # 1. Creative ID 얻기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params, timeout=15)
        creative_response.raise_for_status()
        creative_id = creative_response.json().get('creative', {}).get('id')

        # ★★★ 디버깅 대상 ID인지 확인 ★★★
        is_debug_target = creative_id == DEBUG_CREATIVE_ID

        if creative_id:
            # 2. Creative 상세 정보 얻기
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            fields = ('object_type,image_url,thumbnail_url,video_id,'
                      'object_story_spec{link_data{link,picture,image_url,video_id}},'
                      'asset_feed_spec{videos{video_id,thumbnail_url},images{url},link_urls{website_url}},'
                      'instagram_permalink_url')
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params, timeout=20)
            details_response.raise_for_status()
            details_data = details_response.json() # API 응답 저장

            # ★★★ 디버깅 대상 ID면 API 응답 키 로깅 ★★★
            if is_debug_target:
                print(f"\n--- DEBUG TARGET Creative ID: {creative_id} ---")
                try:
                     print(f"  API Response Keys: {list(details_data.keys())}")
                     # 상세 데이터 필요시 아래 주석 해제 (로그가 너무 길어질 수 있음)
                     # print(f"  API Full Response: {json.dumps(details_data, indent=2)}")
                except Exception as log_e:
                     print(f"  Error logging API response keys: {log_e}")


            # 3. 상세 데이터 추출 (기존과 동일)
            image_url = details_data.get('image_url')
            thumbnail_url = details_data.get('thumbnail_url')
            video_id = details_data.get('video_id')
            story_spec = details_data.get('object_story_spec', {})
            link_data = story_spec.get('link_data', {})
            oss_link = link_data.get('link'); oss_picture_url = link_data.get('picture'); oss_image_url = link_data.get('image_url'); oss_video_id = link_data.get('video_id')
            asset_feed_spec = details_data.get('asset_feed_spec', {})
            videos_from_feed = asset_feed_spec.get('videos', [])
            feed_video_id = videos_from_feed[0].get('video_id') if videos_from_feed else None
            feed_thumbnail_url = videos_from_feed[0].get('thumbnail_url') if videos_from_feed else None
            images_from_feed = asset_feed_spec.get('images', [])
            feed_image_url = images_from_feed[0].get('url') if images_from_feed else None
            link_urls_from_feed = asset_feed_spec.get('link_urls', [])
            feed_website_url = link_urls_from_feed[0].get('website_url') if link_urls_from_feed else None
            instagram_permalink_url = details_data.get('instagram_permalink_url')

            # --- 4. 최종 값 결정 (기존 단순 로직 유지) ---
            actual_video_id = video_id or feed_video_id or oss_video_id
            has_image = image_url or feed_image_url or oss_image_url or oss_picture_url or thumbnail_url or feed_thumbnail_url

            content_type = '알 수 없음'
            if actual_video_id: content_type = '동영상'
            elif has_image: content_type = '사진'
            creative_details['content_type'] = content_type

            display_image_url_options = [image_url, feed_image_url, oss_image_url, oss_picture_url, feed_thumbnail_url, thumbnail_url]
            display_image_url = next((url for url in display_image_url_options if isinstance(url, str) and url.startswith('http')), '')
            creative_details['display_url'] = display_image_url

            creative_asset_url = ''
            if content_type == '동영상' and actual_video_id:
                 video_source_url = get_video_source_url(actual_video_id, ver, token)
                 creative_asset_url = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={actual_video_id}"
            elif content_type == '사진':
                 image_options = [image_url, feed_image_url, oss_image_url, oss_picture_url, thumbnail_url, feed_thumbnail_url]
                 best_image_for_asset = next((url for url in image_options if isinstance(url, str) and url.startswith('http')), '')
                 creative_asset_url = best_image_for_asset
            if isinstance(creative_asset_url, str) and creative_asset_url.startswith('http'): creative_details['creative_asset_url'] = creative_asset_url

            target_url_options = [feed_website_url, oss_link, instagram_permalink_url]
            best_target_url = next((url for url in target_url_options if isinstance(url, str) and url.startswith('http')), '')
            creative_details['target_url'] = best_target_url

            # ★★★ 디버깅 대상 ID면 최종 결과 로깅 ★★★
            if is_debug_target:
                 print(f"  Determined Type: {creative_details['content_type']}")
                 print(f"  Determined Display URL: {creative_details['display_url']}")
                 print(f"  Determined Asset URL: {creative_details['creative_asset_url']}")
                 print(f"--- DEBUG TARGET Creative ID: {creative_id} FINISHED ---\n")

        else: # creative_id is None
             print(f"Warning: Could not get Creative ID for Ad ID: {ad_id}")

    except requests.exceptions.RequestException as e:
        response_text = e.response.text[:500] if hasattr(e, 'response') and e.response is not None else 'N/A'
        print(f"Warning: Network error for Ad ID {ad_id}. Error: {e}. Response: {response_text}...")
        # ★★★ 디버깅 대상 ID에서 네트워크 오류 발생 시 ★★★
        if creative_id == DEBUG_CREATIVE_ID or (not creative_id and ad_id == '관련_Ad_ID_넣기'): # ad_id로도 체크해볼 수 있음
             print(f"--- DEBUG FAILED (Network Error) for Ad ID: {ad_id} ---")
    except Exception as e:
        # ★★★ 디버깅 대상 ID 처리 중 오류 발생 시 상세 로깅 ★★★
        print(f"ERROR: Processing error for Ad ID {ad_id} (Creative ID: {creative_id}). Error Type: {type(e).__name__}, Message: {str(e)}")
        # traceback.print_exc() # 필요 시 전체 트레이스백 출력
        if is_debug_target: # is_debug_target 변수 사용
             print(f"--- DEBUG FAILED (Processing Error) for Creative ID: {creative_id} ---")
             print(f"--- DEBUG State before return in except block: {creative_details}")

    return creative_details
# ↑↑↑↑↑ 위 함수 전체를 복사해서 기존 get_creative_details 함수를 덮어쓰세요 ↑↑↑↑↑
def get_video_source_url(video_id, ver, token):
    """비디오 ID로 원본 비디오 소스 URL을 가져옵니다."""
    # 이전 버전 유지
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params, timeout=10)
        video_response.raise_for_status()
        source_url = video_response.json().get('source')
        # source URL이 유효한지 간단히 확인 (http로 시작하는지)
        if isinstance(source_url, str) and source_url.startswith('http'):
            return source_url
        else:
            # print(f"Notice: Video source URL for {video_id} is invalid: {source_url}")
            return None
    except requests.exceptions.RequestException as e:
        # 비디오 소스 가져오기 실패는 흔할 수 있으므로 Notice 레벨
        # print(f"Notice: Could not fetch video source for video {video_id}. Error: {e}")
        return None # 실패 시 None 반환
    except Exception as e:
        # print(f"Notice: Unexpected error fetching video source for {video_id}. Error: {e}")
        return None


def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    """ThreadPoolExecutor를 사용하여 여러 광고의 크리에이티브 정보를 병렬로 가져옵니다."""
    # 이전 버전 유지
    print(f"Fetching creative details for {len(ad_data)} ads using up to {max_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if not ad_data:
            print("No ad data provided for fetching creatives.")
            return

        # 각 광고 ID에 대해 get_creative_details 함수 실행 요청
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}

        processed_count = 0
        total_ads = len(futures)
        for future in as_completed(futures):
            ad_id = futures[future]
            # 기본 결과 구조 (오류 발생 시 사용)
            creative_info = {'content_type': '오류', 'display_url': '', 'target_url': '', 'creative_asset_url': ''}
            try:
                # 작업 결과 가져오기
                creative_info = future.result()
            except Exception as e:
                # 개별 작업 실패 로깅 (전체 중단 방지)
                print(f"Error processing creative future for ad {ad_id}: {e}")
                # 오류 발생 시 creative_info는 위 기본값('오류') 사용됨

            # 원본 ad_data 딕셔너리에 결과 저장
            if ad_id in ad_data:
                ad_data[ad_id]['creative_details'] = creative_info

            processed_count += 1
            # 진행 상황 로깅 (너무 자주 찍히지 않도록 조절)
            if processed_count % 50 == 0 or processed_count == total_ads:
                 print(f"Processed {processed_count}/{total_ads} creative details...")

    print("Finished fetching creative details.")


# ↓↓↓↓↓ 최종 수정된 fetch_and_format_facebook_ads_data 함수 ↓↓↓↓↓
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals):
    """Meta 광고 데이터를 가져오고, Cafe24 총계를 통합하여 최종 보고서(HTML, JSON)를 생성합니다."""

    all_records = []
    # actions 필드 상세 요청 (value 포함) - 이 부분은 문제 없으므로 유지
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions{action_type,value}'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true',
        'action_attribution_windows': ['1d_click', '7d_click', '1d_view'], # 표준 기여 설정
        'limit': 200 # 페이지당 레코드 수
    }
    page_count = 1
    # 페이지네이션 처리
    while insights_url:
        print(f"Fetching Meta Ads data page {page_count}...")
        # 첫 페이지는 params 사용, 다음 페이지부터는 URL에 포함된 access_token 사용
        current_url = insights_url
        current_params = params if page_count == 1 else None # 다음 페이지는 URL에 파라미터가 포함되어 있음

        try:
            # 타임아웃 늘리기 (60초)
            response = requests.get(url=current_url, params=current_params, timeout=60)
            response.raise_for_status() # 오류 발생 시 예외 발생
        except requests.exceptions.Timeout:
            print(f"Error: Meta Ads API request timed out (Page: {page_count}). Stopping pagination.")
            break # 타임아웃 시 중단
        except requests.exceptions.RequestException as req_err:
            print(f"Error: Meta Ads API network error (Page: {page_count}): {req_err}. Stopping pagination.")
            # 응답 내용 일부 로깅 (디버깅 목적)
            if hasattr(req_err, 'response') and req_err.response is not None:
                 print(f"Response Status: {req_err.response.status_code}")
                 print(f"Response Text: {req_err.response.text[:500]}...") # 너무 길지 않게 일부만
            break # 네트워크 오류 시 중단
        except Exception as e:
            print(f"An unexpected error occurred during API fetch (Page: {page_count}): {e}")
            traceback.print_exc()
            break

        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page:
            # print("No more data found on this page.")
            break # 데이터 없으면 중단

        all_records.extend(records_on_page)
        # print(f"Fetched {len(records_on_page)} records from page {page_count}. Total records: {len(all_records)}")

        # 다음 페이지 URL 가져오기
        insights_url = data.get('paging', {}).get('next')
        page_count += 1
        # params = None # 첫 페이지 이후 params 객체는 사용 안 함 (current_params 로직에서 이미 처리)

    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")

    if not all_records:
        print("No Meta Ads data found for the selected period.")
        empty_html = "<p>선택한 기간에 Meta 광고 데이터가 없습니다.</p>"
        # Cafe24 총계만이라도 포함하는 기본 구조 반환
        return {"html_table": empty_html, "data": [], "cafe24_totals": cafe24_totals} # cafe24_totals 포함

    # 데이터 집계 (ad_id 기준) - actions 처리 로직 개선 적용
    ad_data = {}
    print("Aggregating ad data...")
    for record in all_records:
        ad_id = record.get('ad_id')
        if not ad_id:
            # print("Warning: Record found without ad_id. Skipping.")
            continue # ad_id 없는 레코드는 집계 불가

        if ad_id not in ad_data: # 광고 ID가 처음 나오면 초기화
            ad_data[ad_id] = {
                'ad_id': ad_id,
                'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'),
                'adset_name': record.get('adset_name'),
                'spend': 0.0,
                'impressions': 0,
                'link_clicks': 0, # 'clicks' 필드를 link_clicks 로 사용
                'purchase_count': 0
            }

        # 기본 지표 누적 (spend, impressions, clicks)
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0)) # 'clicks' 사용
        except (ValueError, TypeError): pass

        # 구매 수 집계 (actions 처리 개선 로직)
        actions_data = record.get('actions'); actions_list = [];
        if isinstance(actions_data, dict): actions_list = actions_data.get('data', [])
        elif isinstance(actions_data, list): actions_list = actions_data
        if not isinstance(actions_list, list): actions_list = []

        purchase_count_on_record = 0
        for action in actions_list:
            if not isinstance(action, dict): continue
            action_type = action.get("action_type", "");
            purchase_events = ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase", "website_purchase"]
            if action_type in purchase_events:
                try:
                    value_str = action.get("value", "0"); purchase_count_on_record += int(float(value_str))
                except (ValueError, TypeError): pass # 값 변환 오류 시 해당 action 건너뛰기
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record;

        # 이름 필드 업데이트 (가장 최신 정보로 덮어쓰기)
        if record.get('ad_name'): ad_data[ad_id]['ad_name'] = record.get('ad_name')
        if record.get('campaign_name'): ad_data[ad_id]['campaign_name'] = record.get('campaign_name')
        if record.get('adset_name'): ad_data[ad_id]['adset_name'] = record.get('adset_name')

    print(f"Finished aggregating ad data. Unique ad IDs: {len(ad_data)}")

    # --- 크리에이티브 정보 병렬 조회 (위에서 복원/개선된 함수 호출) ---
    fetch_creatives_parallel(ad_data, ver, token) # ad_data 딕셔너리에 'creative_details'가 추가됨

    result_list = list(ad_data.values()) # DataFrame 생성을 위해 리스트로 변환
    if not result_list:
        print("No valid ad data aggregated.")
        return {"html_table": "<p>Meta 광고 데이터 집계 결과 없음.</p>", "data": [], "cafe24_totals": cafe24_totals}

    df = pd.DataFrame(result_list)
    print("DataFrame created from aggregated ad data.")

    # --- DataFrame 후처리 및 HTML/JSON 생성 (개선된 로직) ---

    # creative_details 컬럼 처리 (딕셔너리에서 개별 컬럼 추출)
    # ★★★ creative_asset_url 추가 ★★★
    default_creative_details = {'content_type': '알 수 없음', 'display_url': '', 'target_url': '', 'creative_asset_url': ''}
    # apply 대신 map 사용 시 lambda 함수에서 get 호출로 안전하게 접근
    df['creative_details_dict'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', default_creative_details))
    # 각 키에 대해 .get()으로 안전하게 추출
    df['콘텐츠 유형'] = df['creative_details_dict'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details_dict'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details_dict'].apply(lambda x: x.get('target_url', '')) # 랜딩 페이지 URL
    df['creative_asset_url'] = df['creative_details_dict'].apply(lambda x: x.get('creative_asset_url', '')) # ★ 소재 원본 URL
    df = df.drop(columns=['creative_details_dict']) # 임시 컬럼 삭제

    # 숫자형 컬럼 처리 및 계산 지표 생성
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)

    # 컬럼명 한글로 변경
    df = df.rename(columns={'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명', 'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click', 'purchase_count': '구매 수'})

    # 정수형으로 표시할 컬럼 타입 변경
    int_cols = ['노출', 'Click', '구매 수', 'CPC', '구매당 비용'];
    for col in int_cols: df[col] = df[col].replace([float('inf'), float('-inf')], 0).round(0).astype(int)
    df['FB 광고비용'] = df['FB 광고비용'].replace([float('inf'), float('-inf')], 0).round(0).astype(int)

    # --- 합계 행 생성 (Cafe24 총계 포함) ---
    # (기존 코드 유지)
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum(); total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '', 'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks, 'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases, '구매당 비용': total_cpp,
        'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), 'Cafe24 매출': cafe24_totals.get('total_sales', 0),
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', 'creative_asset_url': '', '광고 성과': '' # 추가된 컬럼 빈 값으로 초기화
    }
    totals_row = pd.Series(totals_data)

    # 광고 성과 컬럼 생성/확인
    # (기존 코드 유지)
    if 'ad_id' not in df.columns: df['ad_id'] = None
    df['광고 성과'] = '' # 광고 성과 컬럼 초기화

    # URL 매핑 정보 저장 (ad_id 없는 행 제외, 정렬 전에 수행)
    # ★★★ creative_asset_url 추가 ★★★
    df_valid_ad_id = df.dropna(subset=['ad_id'])
    url_map = df_valid_ad_id.set_index('ad_id')[['display_url', 'target_url', 'creative_asset_url']].to_dict('index') if not df_valid_ad_id.empty else {}

    # --- 합계 행 추가 및 정렬 ---
    # (기존 코드 유지)
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1 # 합계 행은 항상 위로
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'], errors='ignore')
    print("DataFrame sorted.")

    # --- 광고 성과 분류 ---
    # (기존 categorize_performance 함수 및 호출 로직 유지)
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy(); df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()

    def categorize_performance(row):
        # (기존 코드 유지)
        if row['광고명'] == '합계': return ''
        ad_id_current = row.get('ad_id')
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        # isinf 체크 전에 NaN 체크 우선
        if pd.isna(cost): return ''
        # math.isinf 사용 전에 cost가 숫자인지 확인 (이미 numeric 변환됨)
        if math.isinf(cost) or cost == 0: return ''
        if cost >= 100000: return '개선 필요!'
        if ad_id_current in top_ad_ids:
            try:
                rank = top_ad_ids.index(ad_id_current)
                if rank == 0: return '위닝 콘텐츠'
                elif rank == 1: return '고성과 콘텐츠'
                elif rank == 2: return '성과 콘텐츠'
            except ValueError: return ''
        return ''

    if 'ad_id' in df_sorted.columns: df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    else: df_sorted['광고 성과'] = ''
    print("Ad performance categorized.")

    # --- URL 재매핑 (정렬 후) ---
    # ★★★ creative_asset_url 추가 ★★★
    if 'ad_id' in df_sorted.columns:
        # map 사용 시 키가 없는 경우 대비하여 .get() 사용
        df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', ''))
        df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', ''))
        df_sorted['creative_asset_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('creative_asset_url', ''))
    else:
        df_sorted['display_url'] = ''
        df_sorted['target_url'] = ''
        df_sorted['creative_asset_url'] = ''

    # --- HTML 테이블 생성 (최종 수정된 로직) ---
    print("Generating HTML table...")
    def format_currency(amount):
        try: return f"{int(amount):,} ₩"
        except (ValueError, TypeError): return "0 ₩"
    def format_number(num):
        try: return f"{int(num):,}"
        except (ValueError, TypeError): return "0"

    display_columns = ['광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', '구매 수', '구매당 비용', 'Cafe24 방문자 수', 'Cafe24 매출', '광고 성과', '콘텐츠 유형', '광고 콘텐츠']
    html_table = """
    <style>
    table {border-collapse: collapse; width: 100%; font-family: sans-serif; font-size: 12px;}
    th, td {padding: 8px; border-bottom: 1px solid #ddd; text-align: right; white-space: nowrap; vertical-align: middle;}
    th {background-color: #f2f2f2; text-align: center; font-weight: bold;}
    td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; } /* 광고명, 캠페인명, 광고세트명 왼쪽 정렬 */
    td:nth-child(11), td:nth-child(12), td:nth-child(13), td:nth-child(14), td:nth-child(15) { text-align: center; } /* Cafe24 방문자 ~ 광고 콘텐츠 가운데 정렬 */
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;} /* 초록색 */
    .medium-performance {color: #E69900; font-weight: bold;} /* 주황색 */
    .third-performance {color: #FF9900; font-weight: bold;} /* 연한 주황색 */
    .needs-improvement {color: #FF0000; font-weight: bold;} /* 빨간색 */
    a {text-decoration: none; color: inherit;}
    img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle; border: 1px solid #eee;} /* 테두리 추가 */
    td.ad-content-cell { text-align: center; }
    </style>
    <table><thead><tr>
    """
    for col_name in display_columns: html_table += f"<th>{col_name}</th>"
    html_table += "</tr></thead><tbody>"

    for index, row in df_sorted.iterrows():
        is_total_row = row.get('광고명') == '합계'; row_class = 'total-row' if is_total_row else ''
        html_table += f'<tr class="{row_class}">'
        for col in display_columns:
            value = None; td_class = []; td_align = 'right' # 기본값

            if col in ['광고명', '캠페인명', '광고세트명']: value = row.get(col, ''); td_align = 'left'; td_class.append('text-left')
            elif col in ['FB 광고비용', 'CPC', '구매당 비용', 'Cafe24 매출']:
                 if not is_total_row and col == 'Cafe24 매출': value = '-'
                 else: value = format_currency(row.get(col))
            elif col in ['노출', 'Click', '구매 수', 'Cafe24 방문자 수']:
                 if not is_total_row and col == 'Cafe24 방문자 수': value = '-'
                 else: value = format_number(row.get(col))
            elif col == 'CTR': value = row.get(col, '0.00%')
            elif col == '광고 성과':
                performance_text = row.get(col, ''); performance_class = '';
                if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
                elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
                elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
                elif performance_text == '개선 필요!': performance_class = 'needs-improvement'
                value = performance_text;
                if performance_class: td_class.append(performance_class)
                td_align = 'center'; td_class.append('text-center')
            elif col == '콘텐츠 유형': value = row.get(col, '-') if not is_total_row else ''; td_align = 'center'; td_class.append('text-center')
            elif col == '광고 콘텐츠':
                # ★★★ 최종 수정: creative_asset_url 만 링크에 사용 ★★★
                display_url = row.get('display_url', '')
                creative_asset_url = row.get('creative_asset_url', '') # 소재 원본 링크
                content_tag = ""

                if not is_total_row and display_url: # 합계 아니고 표시할 썸네일 URL 있으면
                    img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
                    # 링크는 creative_asset_url이 유효할 때만 생성
                    link_url_to_use = creative_asset_url if isinstance(creative_asset_url, str) and creative_asset_url.startswith('http') else ''

                    if link_url_to_use: # 유효한 소재 링크가 있으면 <a> 태그로 감싸기
                        content_tag = f'<a href="{link_url_to_use}" target="_blank" title="광고 소재 보기">{img_tag}</a>'
                    else: # 소재 링크 없으면 이미지만 표시 (링크 없음!)
                        content_tag = img_tag
                elif not is_total_row: # 썸네일 URL 없으면 '-' 표시
                    content_tag = "-"

                value = content_tag; td_class.append("ad-content-cell"); td_align = 'center'
            else: value = row.get(col, '') # 기타 컬럼

            td_style = f'text-align: {td_align};'
            td_class_attr = f' class="{" ".join(td_class)}"' if td_class else ''
            html_table += f'<td{td_class_attr} style="{td_style}">{value}</td>'
        html_table += "</tr>\n"
    html_table += "</tbody></table>"
    print("HTML table generated.")

    # --- JSON 데이터 준비 ---
    print("Preparing JSON data...")
    # JSON에는 광고 콘텐츠(HTML 태그) 제외하고, URL 정보는 포함
    final_columns_for_json = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC',
        '구매 수', '구매당 비용', 'Cafe24 방문자 수', 'Cafe24 매출', '광고 성과', '콘텐츠 유형',
        'ad_id', 'display_url', 'target_url', 'creative_asset_url' # URL 정보 포함
    ]
    # df_sorted에서 필요한 컬럼만 선택 (존재하지 않는 컬럼은 제외)
    df_for_json = df_sorted[[col for col in final_columns_for_json if col in df_sorted.columns]].copy()

    # NaN, Inf, Timestamp 등 JSON 직렬화 불가능한 타입 처리 함수
    def clean_data_for_json(obj):
        if isinstance(obj, dict): return {k: clean_data_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [clean_data_for_json(elem) for elem in obj]
        elif isinstance(obj, (int, float)):
            if pd.isna(obj) or math.isinf(obj): return None # NaN, Inf -> None
            # NumPy 타입을 Python 기본 타입으로 변환 (item() 사용)
            if hasattr(obj, 'item'): return obj.item()
            return obj
        elif isinstance(obj, (pd.Timestamp, date)): return obj.isoformat() # 날짜/시간 -> ISO 문자열
        elif pd.isna(obj): return None # Pandas NA 처리
        elif hasattr(obj, 'item'): # 다른 NumPy 타입 처리 시도
             try: return obj.item()
             except: return str(obj) # 실패 시 문자열 변환
        elif isinstance(obj, (bool, str)) or obj is None: return obj # 기본 타입 및 None 유지
        else: return str(obj) # 그 외는 문자열로 변환

    # DataFrame을 딕셔너리 리스트로 변환 후 클리닝 함수 적용
    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_data_for_json(records)
    print("JSON data prepared.")

    # 최종 결과 반환 (HTML 테이블과 정제된 JSON 데이터)
    return {"html_table": html_table, "data": cleaned_records}


# --- 앱 실행 부분 ---
# Vercel 배포 시에는 이 부분이 직접 실행되지 않음 (Flask 앱 인스턴스 'app'을 사용)
# 로컬 테스트 시 아래 주석 해제 후 실행
# if __name__ == '__main__':
#     # 로컬 테스트 시 환경 변수 로드 확인 필요
#     print("Starting Flask app for local testing...")
#     # load_dotenv() 호출이 상단에 있으므로 여기선 생략 가능
#     # ACCOUNT_CONFIGS = load_account_configs() # 이미 로드됨
#     # CAFE24_CONFIGS = load_cafe24_configs_from_env() # 이미 로드됨
#     print(f"Loaded Meta account configurations: {list(ACCOUNT_CONFIGS.keys())}")
#     print(f"Loaded Cafe24 configurations mapping: {list(CAFE24_CONFIGS.keys())}")
#     # 디버그 모드 활성화, 포트 설정 (환경 변수 PORT 우선 사용)
#     app.run(debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 5001)))
