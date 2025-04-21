# api/index.py
# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
import json # For jsonify

import pandas as pd
import requests
from flask import Flask, jsonify, request
import re

# --- Cafe24 API 모듈 import ---
try:
    # Vercel 환경에서는 상대 경로 임포트 사용
    from . import cafe24_api
    # cafe24_api 모듈에서 로드된 설정과 함수 가져오기
    CAFE24_CONFIGS = cafe24_api.CAFE24_CONFIGS
    process_cafe24_data = cafe24_api.process_cafe24_data
    print("Successfully imported Cafe24 module.")
except ImportError as e:
    # 임포트 실패 시 에러 로깅 및 더미 함수 정의 (앱 비정상 종료 방지)
    print(f"CRITICAL: Could not import cafe24_api module: {e}. Cafe24 features will be disabled.")
    traceback.print_exc()
    CAFE24_CONFIGS = {}
    # 실제 함수와 동일한 반환 구조를 갖는 더미 함수
    def process_cafe24_data(*args, **kwargs):
        print("Executing dummy process_cafe24_data due to import error.")
        return {"visitors": {}, "sales": {}} # 일별 데이터 구조 반환

# .env 파일 로드 (로컬 테스트용)
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env') # api 폴더 밖에 .env 파일이 있을 경우
    if not os.path.exists(dotenv_path):
        dotenv_path = '.env' # 현재 폴더에서 찾기
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)
        print(f"dotenv loaded from {dotenv_path}.")
    else:
        print("dotenv file not found, skipping .env load.")
except ImportError:
    print("dotenv not installed, skipping .env load.")
    pass

app = Flask(__name__)

# --- Meta 계정 설정 로드 ---
def load_account_configs():
    accounts = {}
    i = 1
    while True:
        name = os.environ.get(f"ACCOUNT_CONFIG_{i}_NAME")
        account_id = os.environ.get(f"ACCOUNT_CONFIG_{i}_ID")
        token = os.environ.get(f"ACCOUNT_CONFIG_{i}_TOKEN")
        if name and account_id and token:
            accounts[name] = {"id": account_id, "token": token, "name": name}
            print(f"Loaded Meta account: {name} (ID: {account_id})")
            i += 1
        else:
            if i == 1 and not name: pass
            elif name or account_id or token: print(f"Warning: Incomplete Meta account config index {i}.")
            break
    if not accounts: print("Warning: No complete Meta account configurations found.")
    return accounts

ACCOUNT_CONFIGS = load_account_configs()

# CORS 허용 설정
@app.after_request
def after_request(response):
    # 요청 허용 출처 (필요시 특정 도메인으로 제한)
    origin = request.headers.get('Origin')
    allowed_origins = ['*'] # 모든 도메인 허용 (개발 중)
    # allowed_origins = ['https://your-frontend-domain.com', 'http://localhost:3000'] # 실제 배포 시 프론트엔드 주소 지정

    if origin in allowed_origins or allowed_origins == ['*']:
       response.headers.add('Access-Control-Allow-Origin', origin if origin else '*')

    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true') # 필요 시 추가
    return response

# 기본 경로 및 /api 경로 핸들러
@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook & Cafe24 Ads Report API is running."})

# 계정 목록 반환 API
@app.route('/api/accounts', methods=['POST', 'OPTIONS']) # OPTIONS 메서드 추가
def get_accounts():
    # Preflight 요청 처리
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    try:
        # POST 요청 본문에서 비밀번호 확인 (JSON 형식 가정)
        data = request.get_json()
        if not data: return jsonify({"error": "JSON body required."}), 400

        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")

        # 비밀번호 환경 변수가 설정되어 있고, 입력된 비밀번호가 다르거나 없을 경우 오류 반환
        if report_password and (not password or password != report_password):
             return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 로드된 Meta 계정 이름 목록 반환
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)

    except Exception as e:
        print(f"Error getting account list: {e}"); traceback.print_exc()
        return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API (최종 수정 버전) ---
@app.route('/api/generate-report', methods=['POST', 'OPTIONS']) # OPTIONS 메서드 추가
def generate_report():
    # Preflight 요청 처리
    if request.method == 'OPTIONS':
        return jsonify({}), 200

    try:
        data = request.get_json()
        if not data: return jsonify({"error": "JSON body required."}), 400

        # --- 비밀번호 확인 ---
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # --- 날짜 설정 ---
        today = datetime.now().date()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date_str = data.get('start_date') or default_date
        end_date_str = data.get('end_date') or start_date_str # 종료일 없으면 시작일과 동일하게

        # 날짜 형식 유효성 검사
        try:
            datetime.strptime(start_date_str, '%Y-%m-%d')
            datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

        print(f"Report requested for date range: {start_date_str} to {end_date_str}")

        # --- Meta 계정 선택 ---
        selected_account_key = data.get('selected_account_key')
        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1: selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
            elif len(ACCOUNT_CONFIGS) > 1: return jsonify({"error": f"Meta 계정 키 필요. (사용 가능: {', '.join(ACCOUNT_CONFIGS.keys())})"}), 400
            else: return jsonify({"error": "설정된 Meta 광고 계정 없음."}), 400

        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config: return jsonify({"error": f"선택한 Meta 계정 키 '{selected_account_key}' 설정 없음."}), 404

        meta_account_id = account_config.get('id')
        meta_token = account_config.get('token')
        if not meta_account_id or not meta_token: return jsonify({"error": f"Meta 계정 '{selected_account_key}' 설정 오류."}), 500

        meta_api_version = "v19.0" # Meta API 버전

        # --- 1. Cafe24 총계 데이터 가져오기 ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0} # 기본값 초기화
        daily_cafe24_data = {"visitors": {}, "sales": {}} # 일별 데이터 저장용

        # cafe24_api 모듈 로드 성공 및 설정 존재 여부 확인
        if 'cafe24_api' in globals() and CAFE24_CONFIGS:
            # Meta 계정 키와 매칭되는 Cafe24 설정 찾기 (이름 기반)
            matching_cafe24_config = CAFE24_CONFIGS.get(selected_account_key)

            if matching_cafe24_config:
                print(f"Found matching Cafe24 config for '{selected_account_key}'. Attempting to fetch data...")
                try:
                    # cafe24_api.py의 함수 호출 (일별 데이터 반환)
                    daily_cafe24_data = process_cafe24_data(
                        selected_account_key,    # 설정 키 (이름)
                        matching_cafe24_config, # 해당 설정 딕셔너리
                        start_date_str,            # 시작일
                        end_date_str               # 종료일
                    )

                    # 일별 데이터에서 총계 계산
                    if daily_cafe24_data and isinstance(daily_cafe24_data.get("visitors"), dict) and isinstance(daily_cafe24_data.get("sales"), dict):
                        total_visitors = sum(daily_cafe24_data["visitors"].values())
                        total_sales = sum(daily_cafe24_data["sales"].values())
                        # 최종 집계값 저장 (매출은 정수로)
                        cafe24_totals = {"total_visitors": total_visitors, "total_sales": int(round(total_sales))}
                        print(f"Cafe24 totals calculated. Visitors: {total_visitors}, Sales: {cafe24_totals['total_sales']}")
                    else:
                         print("Warning: process_cafe24_data did not return the expected dictionary structure. Using default totals.")

                except Exception as cafe24_err:
                    print(f"Error occurred during Cafe24 data processing for '{selected_account_key}': {cafe24_err}")
                    traceback.print_exc() # 오류 상세 내용 출력
                    # 오류 발생 시 기본값(0) 유지
            else:
                print(f"No matching Cafe24 configuration found for key '{selected_account_key}'. Skipping Cafe24 totals fetch.")
        else:
            if 'cafe24_api' not in globals(): print("Cafe24 module not loaded.")
            elif not CAFE24_CONFIGS: print("No Cafe24 configurations loaded.")
            print("Skipping Cafe24 totals fetch.")

        # --- 2. Meta 광고 데이터 가져오기 및 최종 보고서 생성 ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        # fetch_and_format_facebook_ads_data 함수는 cafe24_totals 값을 받아서 사용해야 함
        final_report_data = fetch_and_format_facebook_ads_data(
            start_date_str, end_date_str, meta_api_version, meta_account_id, meta_token,
            cafe24_totals # 계산된 Cafe24 총계 전달
        )
        print("Meta Ads data fetch and report generation completed.")

        # --- 3. 결과 반환 ---
        # 최종 결과에 Cafe24 총계는 이미 포함됨 (fetch_and_format_facebook_ads_data 함수 내부에서 처리 가정)
        # 필요 시 일별 데이터도 응답에 포함 가능
        # final_report_data["cafe24_daily"] = daily_cafe24_data

        print("--- Report generation process complete ---")
        return jsonify(final_report_data) # HTML 테이블과 JSON 데이터 포함

    except Exception as e:
        # 예상치 못한 전체 에러 처리
        error_message = "An internal server error occurred during report generation."
        print(f"{error_message} Details: {str(e)}"); traceback.print_exc()
        return jsonify({"error": error_message, "details": str(e)}), 500


# --- 메타 광고 크리에이티브 관련 함수들 ---
# (get_creative_details, get_video_source_url, fetch_creatives_parallel 함수는 이전과 동일하게 유지)
# ... 이 부분에 이전 코드 그대로 붙여넣기 ...
def get_creative_details(ad_id, ver, token):
    # ... 이전 코드 내용 ...
    creative_details = {'content_type': '알 수 없음', 'display_url': '', 'target_url': ''}
    try:
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params, timeout=10)
        creative_response.raise_for_status()
        creative_id = creative_response.json().get('creative', {}).get('id')
        if creative_id:
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            fields = 'object_type,image_url,thumbnail_url,video_id,effective_object_story_id,object_story_spec{link_data{link,picture,image_hash,image_url,video_id}},instagram_permalink_url,asset_feed_spec{videos{video_id,thumbnail_url}}'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params, timeout=15)
            details_response.raise_for_status()
            details_data = details_response.json()

            object_type = details_data.get('object_type')
            video_id = details_data.get('video_id')
            image_url = details_data.get('image_url')
            thumbnail_url = details_data.get('thumbnail_url')
            instagram_permalink_url = details_data.get('instagram_permalink_url')
            story_spec = details_data.get('object_story_spec', {})
            asset_feed_spec = details_data.get('asset_feed_spec', {})
            videos_from_feed = asset_feed_spec.get('videos', [])
            first_video = videos_from_feed[0] if videos_from_feed else {}
            feed_video_id = first_video.get('video_id')
            feed_thumbnail_url = first_video.get('thumbnail_url')
            link_data = story_spec.get('link_data', {})
            oss_image_url = link_data.get('image_url') or link_data.get('picture')
            oss_link = link_data.get('link')
            oss_video_id = link_data.get('video_id')
            actual_video_id = video_id or feed_video_id or oss_video_id

            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else (f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url'])
                else: creative_details['target_url'] = creative_details['display_url']
            elif object_type == 'PHOTO' or image_url or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = oss_link or creative_details['display_url']
            elif object_type == 'SHARE':
                if videos_from_feed or oss_video_id:
                    creative_details['content_type'] = '동영상'; creative_details['display_url'] = feed_thumbnail_url or thumbnail_url or image_url or oss_image_url or ""; share_video_id = feed_video_id or oss_video_id; video_source_url = get_video_source_url(share_video_id, ver, token); creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={share_video_id}"
                elif link_data and (link_data.get('image_hash') or oss_image_url):
                    creative_details['content_type'] = '사진'; creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""; creative_details['target_url'] = oss_link or creative_details['display_url']
                elif instagram_permalink_url:
                    creative_details['content_type'] = '인스타그램'; creative_details['display_url'] = thumbnail_url or image_url or ""; creative_details['target_url'] = instagram_permalink_url
                elif thumbnail_url:
                    creative_details['content_type'] = '동영상'; creative_details['display_url'] = thumbnail_url; story_id = details_data.get('effective_object_story_id'); creative_details['target_url'] = f"https://www.facebook.com/{story_id}" if story_id and "_" in story_id else thumbnail_url
                else: creative_details['content_type'] = '공유 게시물'; creative_details['display_url'] = image_url or thumbnail_url or ""; creative_details['target_url'] = oss_link or creative_details['display_url']
            elif thumbnail_url:
                 creative_details['content_type'] = '사진'; creative_details['display_url'] = thumbnail_url; creative_details['target_url'] = creative_details['display_url']
    except requests.exceptions.Timeout: print(f"Timeout fetching creative details for ad {ad_id}.")
    except requests.exceptions.RequestException as e: print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e: print(f"Error processing creative details for ad {ad_id}: {e}")
    return creative_details

def get_video_source_url(video_id, ver, token):
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"; video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params, timeout=10)
        video_response.raise_for_status(); return video_response.json().get('source')
    except Exception as e: return None # 오류 시 간단히 None 반환

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    print(f"Fetching creative details for {len(ad_data)} ads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for i, future in enumerate(as_completed(futures)):
            ad_id = futures[future]
            try: creative_info = future.result()
            except Exception as e: print(f"Error in creative future for ad {ad_id}: {e}"); creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            if ad_id in ad_data: ad_data[ad_id]['creative_details'] = creative_info
            # 진행 로그 (선택 사항)
            # if (i + 1) % 20 == 0: print(f"  Fetched {i+1}/{len(ad_data)} creatives...")
    print("Finished fetching creative details.")

# --- 메타 광고 데이터 가져오기 및 최종 보고서 생성 함수 ---
# (fetch_and_format_facebook_ads_data 함수는 이전과 동일하게 유지, 단 cafe24_totals 인자를 받도록 함)
# ... 이 부분에 이전 코드 그대로 붙여넣기 (cafe24_totals 인자 받는 부분 확인) ...
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals): # cafe24_totals 인자 추가
    all_records = []
    # 필요한 메트릭 정의 (구매 수 만 필요시 actions 만으로 충분할 수 있음)
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions{action_type,value}'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true', # 통합 기여 설정 사용
        'action_attribution_windows': ['1d_click', '7d_click', '1d_view'], # 필요시 기여 기간 조정
        'limit': 200 # 페이지당 레코드 수 (최대 500까지 가능)
    }
    page_count = 1
    while insights_url:
        print(f"Fetching Meta Ads data page {page_count}...")
        current_url = insights_url if page_count > 1 else insights_url
        current_params = params if page_count == 1 else {'access_token': token} # 다음 페이지는 URL에 파라미터 포함

        try:
            response = requests.get(url=current_url, params=current_params, timeout=60) # 타임아웃 증가
            response.raise_for_status() # HTTP 에러 발생 시 예외
        except requests.exceptions.Timeout:
            print(f"Meta Ads API request timed out (Page: {page_count}). Moving to next step with fetched data.")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Meta Ads API network error (Page: {page_count}): {req_err}")
            # 오류 발생 시 현재까지 수집된 데이터로 진행하거나, 에러 반환 결정 필요
            break # 또는 return {"error": ...}

        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page: break # 데이터 없으면 중단

        all_records.extend(records_on_page)
        paging = data.get('paging', {})
        insights_url = paging.get('next') # 다음 페이지 URL
        page_count += 1
        params = None # 다음 요청부터는 next URL 사용

    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records:
        # Meta 광고 데이터가 없을 때 Cafe24 총계만이라도 보여주기
        empty_html = "<p>선택한 기간에 Meta 광고 데이터가 없습니다.</p>"
        totals_row_only = pd.DataFrame([{
             '광고명': '합계', '캠페인명': '', '광고세트명': '',
             'FB 광고비용': 0, '노출': 0, 'Click': 0, 'CTR': '0.00%', 'CPC': 0,
             '구매 수': 0, '구매당 비용': 0,
             'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0),
             'Cafe24 매출': cafe24_totals.get('total_sales', 0),
             '광고 성과': '', '콘텐츠 유형': ''
        }])
        # totals_row_only 에서 필요없는 컬럼 제거 및 포맷팅 적용 필요
        # 여기서는 간단히 메시지만 반환
        return {"html_table": empty_html, "data": [], "cafe24_totals": cafe24_totals}


    # 데이터 집계 (ad_id 기준)
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue

        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id,
                'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'),
                'adset_name': record.get('adset_name'),
                'spend': 0.0,
                'impressions': 0,
                'link_clicks': 0, # 'clicks' 필드는 모든 종류의 클릭 포함, 'link_clicks'는 별도 요청 필요하거나 actions 에서 찾아야 함
                'purchase_count': 0
            }

        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0)) # 우선 'clicks' 사용, 필요 시 수정
        except (ValueError, TypeError): pass

        # 구매 수 집계 (actions 필드 활용)
        purchase_count_on_record = 0
        actions = record.get('actions', {}).get('data', []) # actions 구조 확인 필요
        if isinstance(actions, list):
            for action in actions:
                # 'purchase' 액션 타입 확인 (실제 action_type 이름 확인 필요)
                if action.get("action_type") == "purchase" or action.get("action_type") == "offsite_conversion.fb_pixel_purchase":
                    try: purchase_count_on_record += int(action.get("value", 0))
                    except (ValueError, TypeError): pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record

        # 이름 필드는 최신 레코드로 업데이트 (동일 ad_id 에 대해 여러 레코드 있을 경우 대비)
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # 크리에이티브 정보 병렬 조회
    fetch_creatives_parallel(ad_data, ver, token)
    result_list = list(ad_data.values());
    if not result_list: return {"html_table": "<p>Meta 데이터 집계 결과 없음.</p>", "data": []} # 집계 후 데이터 없을 경우

    # DataFrame 생성
    df = pd.DataFrame(result_list)

    # 크리에이티브 컬럼 추가
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '-'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details']) # 원본 딕셔너리 컬럼 제거

    # 숫자형 변환 및 기본값 처리
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 계산 지표 생성
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)

    # 컬럼 이름 변경
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click', 'purchase_count': '구매 수'
    })

    # 정수형 변환 (금액 제외)
    int_cols = ['노출', 'Click', '구매 수', 'CPC', '구매당 비용']
    for col in int_cols: df[col] = df[col].round(0).astype(int)
    df['FB 광고비용'] = df['FB 광고비용'].round(0).astype(int) # 광고 비용도 정수로


    # --- 합계 행 계산 (Cafe24 총계 포함) ---
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0

    # 합계 행 데이터 생성 (Cafe24 총계 사용)
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '',
        'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks,
        'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases, '구매당 비용': total_cpp,
        'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), # 전달받은 Cafe24 총계 사용
        'Cafe24 매출': cafe24_totals.get('total_sales', 0),       # 전달받은 Cafe24 총계 사용
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': '' # 나머지 컬럼 기본값
    }
    totals_row = pd.Series(totals_data)

    # --- 데이터 정렬 및 광고 성과 분류 ---
    # 광고 성과 컬럼 미리 생성
    df['광고 성과'] = ''
    # 합계 행 추가 전 URL 매핑 정보 저장
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}

    # 합계 행 추가
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

    # 정렬 키 생성 (구매당 비용 기준, 합계 행은 최상단, 비용 0 또는 NaN은 최하단)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1 # 합계 행은 항상 위로
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        # 구매당 비용이 0이거나 계산 불가능하면 맨 아래로
        return float('inf') if pd.isna(cost) or cost == 0 else cost

    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    # 정렬 실행
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'], errors='ignore')

    # 광고 성과 분류 (정렬된 데이터프레임 기준, 합계 행 제외)
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    # 유효한 구매당 비용(0 초과)을 가진 광고만 순위 선정 후보
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        # 구매당 비용 10만원 미만 광고 중 상위 3개 선정 (기준은 필요에 따라 조정)
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()

    def categorize_performance(row):
        if row['광고명'] == '합계': return '' # 합계 행은 제외
        ad_id_current = row.get('ad_id');
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        if pd.isna(cost) or cost == 0: return ''; # 구매당 비용 계산 불가 시 제외

        if cost >= 100000: return '개선 필요!' # 10만원 이상은 개선 필요

        # 상위 3개 광고에 순위 부여
        if ad_id_current in top_ad_ids:
            try: rank = top_ad_ids.index(ad_id_current)
            except ValueError: return '' # 혹시 모를 오류 방지
            if rank == 0: return '위닝 콘텐츠';
            if rank == 1: return '고성과 콘텐츠';
            if rank == 2: return '성과 콘텐츠';
        return '' # 그 외는 빈칸

    # 정렬된 DataFrame에 광고 성과 적용
    if 'ad_id' in df_sorted.columns:
        df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    else:
        df_sorted['광고 성과'] = '' # ad_id 없으면 적용 불가

    # URL 재매핑 (정렬 후에도 유지되도록)
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', '')) if 'ad_id' in df_sorted.columns else ''
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', '')) if 'ad_id' in df_sorted.columns else ''


    # --- HTML 테이블 생성 (Cafe24 컬럼 포함) ---
    # 포맷 함수 정의
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) else "0"

    # 최종 HTML 테이블에 표시할 컬럼 순서 (ad_id, URL 컬럼 제외)
    display_columns = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용',
        'Cafe24 방문자 수', 'Cafe24 매출', # Cafe24 총계 컬럼
        '광고 성과', '콘텐츠 유형', '광고 콘텐츠' # 광고 성과 및 콘텐츠 관련 컬럼
    ]

    # HTML 테이블 시작 및 CSS (스타일은 필요에 맞게 수정)
    html_table = """
    <style>
        table { border-collapse: collapse; width: 100%; font-family: sans-serif; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: right; }
        th { background-color: #f2f2f2; text-align: center; }
        td.text-left { text-align: left; }
        td.text-center { text-align: center; }
        tr.total-row { font-weight: bold; background-color: #e9e9e9; }
        .ad-content-thumbnail { max-width: 60px; max-height: 60px; vertical-align: middle; }
        .ad-content-cell { width: 80px; text-align: center; }
        /* 광고 성과별 스타일 */
        .winning-content { background-color: #d4edda; color: #155724; font-weight: bold; }
        .medium-performance { background-color: #fff3cd; color: #856404; }
        .third-performance { background-color: #e2e3e5; color: #383d41; }
        .needs-improvement { background-color: #f8d7da; color: #721c24; font-weight: bold; }
    </style>
    <table><thead><tr>
    """
    # HTML 헤더 생성
    for col_name in display_columns: html_table += f"<th>{col_name}</th>"
    html_table += "</tr></thead><tbody>"

    # HTML 바디 생성 (정렬된 DataFrame 사용)
    for index, row in df_sorted.iterrows():
        is_total_row = row.get('광고명') == '합계'
        row_class = 'total-row' if is_total_row else ''
        html_table += f'<tr class="{row_class}">'

        for col in display_columns:
            value = None; td_class = []; td_align = 'right' # 기본 오른쪽 정렬

            # 컬럼별 값 처리 및 클래스/정렬 설정
            if col in ['광고명', '캠페인명', '광고세트명']:
                value = row.get(col, ''); td_align = 'left'; td_class.append('text-left')
            elif col in ['FB 광고비용', 'CPC', '구매당 비용', 'Cafe24 매출']: # Cafe24 매출도 통화 포맷
                value = format_currency(row.get(col)) if not is_total_row or col != 'Cafe24 매출' else format_currency(cafe24_totals.get('total_sales', 0)) # 합계행 Cafe24 매출은 totals 사용
                # 합계 행이 아니거나, 합계 행이면서 Cafe24 관련 컬럼이면 '-' 표시하지 않음
                if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'
                elif is_total_row and col == 'Cafe24 매출': value = format_currency(row.get(col))
            elif col in ['노출', 'Click', '구매 수', 'Cafe24 방문자 수']: # Cafe24 방문자 수도 숫자 포맷
                value = format_number(row.get(col)) if not is_total_row or col != 'Cafe24 방문자 수' else format_number(cafe24_totals.get('total_visitors', 0)) # 합계행 Cafe24 방문자는 totals 사용
                if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'
                elif is_total_row and col == 'Cafe24 방문자 수': value = format_number(row.get(col))
            elif col == 'CTR': value = row.get(col, '0.00%')
            elif col == '광고 성과':
                performance_text = row.get(col, '')
                performance_class = ''
                if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
                elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
                elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
                elif performance_text == '개선 필요!': performance_class = 'needs-improvement'
                value = performance_text
                if performance_class: td_class.append(performance_class)
                td_align = 'center'; td_class.append('text-center')
            elif col == '콘텐츠 유형':
                value = row.get(col, '-') if not is_total_row else ''
                td_align = 'center'; td_class.append('text-center')
            elif col == '광고 콘텐츠':
                display_url = row.get('display_url', ''); target_url = row.get('target_url', '')
                content_tag = ""
                if not is_total_row and display_url:
                    img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
                    if isinstance(target_url, str) and target_url.startswith('http'):
                        content_tag = f'<a href="{target_url}" target="_blank" title="콘텐츠 보기">{img_tag}</a>'
                    else: content_tag = img_tag
                elif not is_total_row: content_tag = "-" # 합계 아니고 URL 없으면 '-'
                value = content_tag
                td_class.append("ad-content-cell"); td_align = 'center'
            else: value = row.get(col, '') # 기타 컬럼 기본값

            # TD 태그 생성 (정렬 및 클래스 적용)
            td_style = f'text-align: {td_align};'
            td_class_attr = f' class="{" ".join(td_class)}"' if td_class else ''
            html_table += f'<td{td_class_attr} style="{td_style}">{value}</td>'

        html_table += "</tr>\n"
    html_table += "</tbody></table>"

    # --- JSON 데이터 준비 (클리닝 포함) ---
    # HTML 테이블에 표시된 순서와 유사하게 JSON 데이터 생성 (광고 콘텐츠 제외)
    final_columns_for_json = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용',
        'Cafe24 방문자 수', 'Cafe24 매출',
        '광고 성과', '콘텐츠 유형', 'ad_id' # ad_id 는 포함시켜 클라이언트에서 활용 가능
    ]
    # 필요한 컬럼만 선택하고, NaN/Inf 등 JSON 직렬화 불가능한 값 처리
    df_for_json = df_sorted[final_columns_for_json].copy()

    # 데이터 클리닝 함수 (NaN, Inf, NumPy 타입 등 처리)
    def clean_data_for_json(obj):
        if isinstance(obj, dict):
            return {k: clean_data_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_data_for_json(elem) for elem in obj]
        elif isinstance(obj, (int, float)):
            if pd.isna(obj) or math.isinf(obj): return None # 또는 0
            # NumPy 타입을 Python 기본 타입으로 변환
            if hasattr(obj, 'item'): return obj.item()
            return obj
        elif isinstance(obj, (pd.Timestamp, date)):
             return obj.isoformat() # 날짜/시간은 ISO 형식 문자열로
        elif hasattr(obj, 'item'): # Pandas/NumPy 객체 처리
             try: return obj.item()
             except: return str(obj) # 변환 실패 시 문자열로
        # bool, str 등은 그대로 반환
        elif isinstance(obj, (bool, str)) or obj is None:
             return obj
        # 그 외 타입은 문자열로 변환 (안전장치)
        else:
             return str(obj)

    # DataFrame을 레코드 리스트로 변환 후 클리닝
    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_data_for_json(records)

    # 최종 결과 반환 (HTML 테이블 + JSON 데이터)
    return {"html_table": html_table, "data": cleaned_records}


# --- 앱 실행 (Vercel 환경에서는 이 부분이 실행되지 않음) ---
# if __name__ == '__main__':
#     # 로컬 테스트 시 사용할 포트 지정 가능
#     app.run(debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 5001)))
