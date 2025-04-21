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
    origin = request.headers.get('Origin')
    # 실제 서비스 시에는 허용할 프론트엔드 도메인 목록을 명시하는 것이 안전합니다.
    allowed_origins = ['*'] # 개발 편의상 모든 도메인 허용 (*)
    # allowed_origins = ['https://your-frontend.com', 'http://localhost:3000'] # 예시
    if origin in allowed_origins or '*' in allowed_origins:
        response.headers.add('Access-Control-Allow-Origin', origin if origin else '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    # response.headers.add('Access-Control-Allow-Credentials', 'true') # 필요 시
    return response

# 기본 경로 및 /api 경로 핸들러
@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook & Cafe24 Ads Report API is running."})

# 계정 목록 반환 API
@app.route('/api/accounts', methods=['POST', 'OPTIONS'])
def get_accounts():
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json()
        if not data: return jsonify({"error": "JSON body required."}), 400
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
             return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)
    except Exception as e:
        print(f"Error getting account list: {e}"); traceback.print_exc()
        return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API ---
@app.route('/api/generate-report', methods=['POST', 'OPTIONS'])
def generate_report():
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json()
        if not data: return jsonify({"error": "JSON body required."}), 400

        # 비밀번호 확인
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 날짜 설정
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

        # Meta 계정 선택
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
        meta_api_version = "v19.0"

        # --- 1. Cafe24 총계 데이터 가져오기 ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0}
        daily_cafe24_data = {"visitors": {}, "sales": {}}
        if 'cafe24_api' in globals() and CAFE24_CONFIGS:
            matching_cafe24_config = CAFE24_CONFIGS.get(selected_account_key)
            if matching_cafe24_config:
                print(f"Found matching Cafe24 config for '{selected_account_key}'. Attempting to fetch data...")
                try:
                    daily_cafe24_data = process_cafe24_data(
                        selected_account_key, matching_cafe24_config, start_date_str, end_date_str
                    )
                    if daily_cafe24_data and isinstance(daily_cafe24_data.get("visitors"), dict) and isinstance(daily_cafe24_data.get("sales"), dict):
                        total_visitors = sum(daily_cafe24_data["visitors"].values())
                        total_sales = sum(daily_cafe24_data["sales"].values())
                        cafe24_totals = {"total_visitors": total_visitors, "total_sales": int(round(total_sales))}
                        print(f"Cafe24 totals calculated. Visitors: {total_visitors}, Sales: {cafe24_totals['total_sales']}")
                    else: print("Warning: process_cafe24_data returned unexpected structure.")
                except Exception as cafe24_err:
                    print(f"Error during Cafe24 data processing for '{selected_account_key}': {cafe24_err}")
                    traceback.print_exc()
            else: print(f"No matching Cafe24 config found for key '{selected_account_key}'. Skipping Cafe24 totals fetch.")
        else: print("Cafe24 module/config not loaded. Skipping Cafe24 totals fetch.")

        # --- 2. Meta 광고 데이터 가져오기 및 최종 보고서 생성 ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        final_report_data = fetch_and_format_facebook_ads_data(
            start_date_str, end_date_str, meta_api_version, meta_account_id, meta_token,
            cafe24_totals # Cafe24 총계 전달
        )
        print("Meta Ads data fetch and report generation completed.")

        # --- 3. 결과 반환 ---
        print("--- Report generation process complete ---")
        return jsonify(final_report_data)

    except Exception as e:
        error_message = "An internal server error occurred during report generation."
        print(f"{error_message} Details: {str(e)}"); traceback.print_exc()
        return jsonify({"error": error_message, "details": str(e)}), 500


# === 사용자가 제공한 이전 버전의 get_creative_details 함수 ===
# 이 버전은 요청하는 필드가 적어서 API 오류 발생 가능성이 낮지만,
# 콘텐츠 유형 판별이나 URL 추출 정확도는 다소 떨어질 수 있습니다.
def get_creative_details(ad_id, ver, token):
    """
    광고 ID를 사용하여 크리에이티브 상세 정보 (콘텐츠 유형, 표시 URL, 대상 URL)를 가져옵니다.
    (사용자 제공 버전 기반)
    """
    creative_details = {
        'content_type': '알 수 없음', # 기본값
        'display_url': '',
        'target_url': ''
    }
    creative_id = None # 초기화
    try:
        # 1. 광고 ID로 크리에이티브 ID 가져오기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        # 필드 명시성 개선: creative{id}
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params, timeout=10) # 타임아웃 추가
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        # .get()을 안전하게 사용하여 creative_id 추출
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            # 2. 크리에이티브 ID로 상세 정보 가져오기 (사용자 제공 필드 사용)
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # 사용자 제공 버전의 필드 목록
            fields = 'object_type,image_url,thumbnail_url,video_id,object_story_spec{link_data{image_hash,image_url},photo_data{image_hash,image_url},video_data{video_id,image_url}}'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params, timeout=15) # 타임아웃 추가
            details_response.raise_for_status()
            details_data = details_response.json()

            # --- [로깅 추가] API 응답 확인용 (선택 사항) ---
            # print(f"--- Creative Details RAW RESPONSE (User Version) for ad_id: {ad_id} (creative_id: {creative_id}) ---")
            # try: print(json.dumps(details_data, indent=2, ensure_ascii=False))
            # except Exception as log_err: print(f"(Error logging raw details: {log_err}) Raw data: {details_data}")
            # print(f"--- END RAW RESPONSE (User Version) for ad_id: {ad_id} ---")
            # --- [로깅 추가] 끝 ---


            # 응답 데이터 파싱 (사용자 제공 버전 로직 기반)
            object_type = details_data.get('object_type')
            video_id = details_data.get('video_id')
            image_url = details_data.get('image_url')
            thumbnail_url = details_data.get('thumbnail_url')
            story_spec = details_data.get('object_story_spec', {}) # 없으면 빈 딕셔너리
            oss_video_id = story_spec.get('video_data', {}).get('video_id')
            oss_image_url = (
                story_spec.get('photo_data', {}).get('image_url') or
                story_spec.get('link_data', {}).get('image_url') or
                story_spec.get('video_data', {}).get('image_url') # 비디오 썸네일 대체
            )

            # 유형 판별 로직 (사용자 제공 버전)
            if object_type == 'VIDEO':
                creative_details['content_type'] = '동영상'
                actual_video_id = video_id or oss_video_id
                creative_details['display_url'] = thumbnail_url or image_url or oss_image_url or ""
                # target_url 은 video watch 링크 또는 display_url
                creative_details['target_url'] = f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url']
            elif object_type == 'PHOTO':
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = creative_details['display_url'] # 사진은 자체 URL
            elif video_id or oss_video_id: # object_type 불명확해도 비디오 ID 있으면 동영상
                creative_details['content_type'] = '동영상'
                actual_video_id = video_id or oss_video_id
                creative_details['display_url'] = thumbnail_url or image_url or oss_image_url or ""
                creative_details['target_url'] = f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url']
            elif image_url or oss_image_url or thumbnail_url: # 이미지 URL 있으면 사진
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = creative_details['display_url']
            # 기타 유형은 '알 수 없음' 유지

        else: # creative_id 못 가져온 경우
            print(f"Warning: Could not retrieve creative_id for ad_id: {ad_id}. Response: {creative_data}")

    except requests.exceptions.Timeout:
         print(f"Timeout fetching creative details for ad_id {ad_id}.")
    except requests.exceptions.RequestException as e:
        response_text = e.response.text[:200] if hasattr(e, 'response') and e.response is not None else 'N/A'
        print(f"Error fetching creative details for ad {ad_id}: {e}. Response snippet: {response_text}...")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")
        # traceback.print_exc() # 상세 디버깅 필요 시 주석 해제

    return creative_details
# === 사용자 제공 버전의 get_creative_details 함수 끝 ===


def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    """ThreadPoolExecutor를 사용하여 크리에이티브 정보 병렬 조회 (변경 없음)"""
    print(f"Fetching creative details for {len(ad_data)} ads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # get_creative_details 함수가 사용자 제공 버전으로 교체되었으므로, 해당 함수가 실행됨
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        processed_count = 0
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception as e:
                print(f"Error getting result for creative future (ad_id: {ad_id}): {e}")
                creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''} # 오류 시 기본값

            # ad_data 딕셔너리에 결과 저장
            if ad_id in ad_data:
                ad_data[ad_id]['creative_details'] = creative_info
            processed_count += 1
            # 진행 상황 로깅 (선택 사항)
            # if processed_count % 20 == 0: print(f"  Fetched {processed_count}/{len(ad_data)} creatives...")
    print(f"Finished fetching creative details. Processed: {processed_count}")


# --- 메타 광고 데이터 가져오기 및 최종 보고서 생성 함수 ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals): # cafe24_totals 인자 유지
    all_records = []
    # 요청 필드에 actions{action_type,value} 포함 확인
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions{action_type,value}'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics, 'access_token': token, 'level': 'ad',
        'time_range[since]': start_date, 'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true',
        'limit': 200
    }
    page_count = 1
    while insights_url:
        print(f"Fetching Meta Ads data page {page_count}...")
        current_url = insights_url if page_count > 1 else insights_url
        current_params = None if page_count > 1 else params # 첫 페이지는 params 사용, 이후는 next url 사용
        try:
            response = requests.get(url=current_url, params=current_params, timeout=60)
            response.raise_for_status()
        except requests.exceptions.RequestException as req_err:
            print(f"Meta Ads API network error (Page: {page_count}): {req_err}")
            break # 오류 발생 시 중단하고 현재까지 데이터로 진행
        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page: break
        all_records.extend(records_on_page)
        paging = data.get('paging', {}); insights_url = paging.get('next')
        page_count += 1
        # 다음 페이지 요청을 위해 params 초기화 (next url 사용)
        params = {'access_token': token} if insights_url else None

    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records:
        # Meta 데이터 없을 경우 처리 (Cafe24 데이터만 표시)
        empty_html = "<p>선택한 기간에 Meta 광고 데이터가 없습니다.</p>"
        # Cafe24 총계만 포함된 데이터 반환 (필요시 구조 조정)
        return {"html_table": empty_html, "data": [], "cafe24_totals": cafe24_totals} # cafe24_totals 키 추가

    # --- 데이터 집계 (ad_id 기준, 수정된 actions 처리 포함) ---
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue

        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id, 'ad_name': record.get('ad_name'), 'campaign_name': record.get('campaign_name'),
                'adset_name': record.get('adset_name'), 'spend': 0.0, 'impressions': 0,
                'link_clicks': 0, 'purchase_count': 0
            }

        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0)) # 'clicks' 사용
        except (ValueError, TypeError): pass

        # === actions 처리 (이전 수정된 내용 유지) ===
        actions_data = record.get('actions')
        actions_list = []
        if isinstance(actions_data, dict): actions_list = actions_data.get('data', [])
        elif isinstance(actions_data, list): actions_list = actions_data
        if not isinstance(actions_list, list): actions_list = []

        purchase_count_on_record = 0
        for action in actions_list:
            if not isinstance(action, dict): continue
            action_type = action.get("action_type", "")
            if action_type == "purchase" or action_type == "offsite_conversion.fb_pixel_purchase" or action_type == "omni_purchase":
                try:
                    value_str = action.get("value", "0")
                    purchase_count_on_record += int(float(value_str))
                except (ValueError, TypeError): pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record
        # === actions 처리 끝 ===

        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # --- 크리에이티브 정보 병렬 가져오기 (사용자 버전 get_creative_details 사용) ---
    fetch_creatives_parallel(ad_data, ver, token)
    result_list = list(ad_data.values())
    if not result_list: return {"html_table": "<p>Meta 데이터 집계 결과 없음.</p>", "data": [], "cafe24_totals": cafe24_totals}

    # DataFrame 생성 및 처리 (컬럼 이름 변경, 계산 지표 생성 등)
    df = pd.DataFrame(result_list)
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음')) # 기본값 '알 수 없음'
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details']) # 처리 후 원본 딕셔너리 제거

    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click', 'purchase_count': '구매 수'
    })
    int_cols = ['노출', 'Click', '구매 수', 'CPC', '구매당 비용']
    for col in int_cols: df[col] = df[col].round(0).astype(int)
    df['FB 광고비용'] = df['FB 광고비용'].round(0).astype(int)

    # --- 합계 행 계산 (Cafe24 총계 포함) ---
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '', 'FB 광고비용': total_spend, '노출': total_impressions,
        'Click': total_clicks, 'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases, '구매당 비용': total_cpp,
        'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), # Cafe24 총계 사용
        'Cafe24 매출': cafe24_totals.get('total_sales', 0),       # Cafe24 총계 사용
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''
    }
    totals_row = pd.Series(totals_data)

    # --- 데이터 정렬 및 광고 성과 분류 ---
    df['광고 성과'] = ''
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'], errors='ignore')

    # 광고 성과 분류
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()
    def categorize_performance(row):
        if row['광고명'] == '합계': return ''
        ad_id_current = row.get('ad_id'); cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        if pd.isna(cost) or cost == 0: return '';
        if cost >= 100000: return '개선 필요!'
        if ad_id_current in top_ad_ids:
            try: rank = top_ad_ids.index(ad_id_current)
            except ValueError: return ''
            if rank == 0: return '위닝 콘텐츠';
            if rank == 1: return '고성과 콘텐츠';
            if rank == 2: return '성과 콘텐츠';
        return ''
    if 'ad_id' in df_sorted.columns: df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    else: df_sorted['광고 성과'] = ''

    # URL 재매핑
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', '')) if 'ad_id' in df_sorted.columns else ''
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', '')) if 'ad_id' in df_sorted.columns else ''


    # --- HTML 테이블 생성 ---
    # (이전과 동일한 로직 사용)
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) else "0"
    display_columns = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용',
        'Cafe24 방문자 수', 'Cafe24 매출',
        '광고 성과', '콘텐츠 유형', '광고 콘텐츠'
    ]
    html_table = """
    <style>
        /* 스타일은 필요에 따라 조정 */
        table { border-collapse: collapse; width: 100%; font-family: sans-serif; font-size: 12px; }
        th, td { border: 1px solid #ddd; padding: 6px; text-align: right; vertical-align: middle; }
        th { background-color: #f2f2f2; text-align: center; white-space: nowrap; }
        td.text-left { text-align: left; }
        td.text-center { text-align: center; }
        tr.total-row { font-weight: bold; background-color: #e9e9e9; }
        .ad-content-thumbnail { max-width: 70px; max-height: 70px; vertical-align: middle; }
        .ad-content-cell { width: 90px; text-align: center; }
        /* 광고 성과 스타일 */
        .winning-content { background-color: #d4edda !important; color: #155724 !important; font-weight: bold; }
        .medium-performance { background-color: #fff3cd !important; color: #856404 !important; }
        .third-performance { background-color: #e2e3e5 !important; color: #383d41 !important; }
        .needs-improvement { background-color: #f8d7da !important; color: #721c24 !important; font-weight: bold; }
        /* 테이블 셀 내 링크 스타일 */
         td a { color: #007bff; text-decoration: none; }
         td a:hover { text-decoration: underline; }
    </style>
    <table><thead><tr>
    """
    for col_name in display_columns: html_table += f"<th>{col_name}</th>"
    html_table += "</tr></thead><tbody>"
    for index, row in df_sorted.iterrows():
        is_total_row = row.get('광고명') == '합계'
        row_class = 'total-row' if is_total_row else ''
        html_table += f'<tr class="{row_class}">'
        for col in display_columns:
            value = None; td_class = []; td_align = 'right'
            if col in ['광고명', '캠페인명', '광고세트명']:
                value = row.get(col, ''); td_align = 'left'; td_class.append('text-left')
            elif col in ['FB 광고비용', 'CPC', '구매당 비용', 'Cafe24 매출']:
                value = format_currency(row.get(col))
                if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'
            elif col in ['노출', 'Click', '구매 수', 'Cafe24 방문자 수']:
                value = format_number(row.get(col))
                if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'
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
                content_tag = "";
                if not is_total_row and display_url:
                    img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일" loading="lazy">' # lazy loading 추가
                    # target_url 이 유효한 http(s) 링크인지 확인
                    if isinstance(target_url, str) and target_url.startswith('http'):
                        content_tag = f'<a href="{target_url}" target="_blank" title="콘텐츠 보기">{img_tag}</a>'
                    else: content_tag = img_tag # 링크 없으면 이미지만 표시
                elif not is_total_row: content_tag = "-"
                value = content_tag
                td_class.append("ad-content-cell"); td_align = 'center'
            else: value = row.get(col, '')

            td_style = f'text-align: {td_align};'
            td_class_attr = f' class="{" ".join(td_class)}"' if td_class else ''
            html_table += f'<td{td_class_attr} style="{td_style}">{value}</td>'
        html_table += "</tr>\n"
    html_table += "</tbody></table>"

    # --- JSON 데이터 준비 ---
    final_columns_for_json = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC',
        '구매 수', '구매당 비용', 'Cafe24 방문자 수', 'Cafe24 매출', '광고 성과', '콘텐츠 유형', 'ad_id'
    ]
    df_for_json = df_sorted[final_columns_for_json].copy()
    def clean_data_for_json(obj): # 이전 클리닝 함수 사용
        if isinstance(obj, dict): return {k: clean_data_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [clean_data_for_json(elem) for elem in obj]
        elif isinstance(obj, (int, float)):
            if pd.isna(obj) or math.isinf(obj): return None
            if hasattr(obj, 'item'): return obj.item()
            return obj
        elif isinstance(obj, (pd.Timestamp, date)): return obj.isoformat()
        elif hasattr(obj, 'item'):
             try: return obj.item()
             except: return str(obj)
        elif isinstance(obj, (bool, str)) or obj is None: return obj
        else: return str(obj)
    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_data_for_json(records)

    # 최종 결과 반환
    return {"html_table": html_table, "data": cleaned_records}


# --- 앱 실행 (Vercel 환경에서는 사용되지 않음) ---
# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 5001)))
