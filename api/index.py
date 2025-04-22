# api/index.py
# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date # date 추가
import json # For jsonify

import pandas as pd
import requests
from flask import Flask, jsonify, request
import re

# --- Cafe24 API 모듈 import (최신 유지) ---
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
        return {"visitors": {}, "sales": {}} # Match return structure

# .env 파일 로드 (로컬 테스트용, 최신 유지)
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

# --- Meta 계정 설정 로드 (최신 유지) ---
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

# CORS 허용 설정 (최신 유지)
@app.after_request
def after_request(response):
    origin = request.headers.get('Origin')
    allowed_origins = ['*'] # 필요시 실제 프론트엔드 주소로 변경
    if origin in allowed_origins or allowed_origins == ['*']:
       response.headers.add('Access-Control-Allow-Origin', origin if origin else '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true') # 필요 시 추가
    return response

# 기본 경로 및 /api 경로 핸들러 (최신 유지)
@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook & Cafe24 Ads Report API is running."})

# 계정 목록 반환 API (최신 유지)
@app.route('/api/accounts', methods=['POST', 'OPTIONS']) # OPTIONS 메서드 추가
def get_accounts():
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json();
        if not data: return jsonify({"error": "JSON body required."}), 400
        password = data.get('password'); report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
             return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403
        account_names = list(ACCOUNT_CONFIGS.keys()); return jsonify(account_names)
    except Exception as e: print(f"Error getting account list: {e}"); traceback.print_exc(); return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API (최신 유지) ---
@app.route('/api/generate-report', methods=['POST', 'OPTIONS']) # OPTIONS 메서드 추가
def generate_report():
    # ... (generate_report 함수 앞부분은 이전과 동일하게 유지) ...
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json();
        if not data: return jsonify({"error": "JSON body required."}), 400
        password = data.get('password'); report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password): return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        today = datetime.now().date(); default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date_str = data.get('start_date') or default_date; end_date_str = data.get('end_date') or start_date_str # 종료일 없으면 시작일과 동일하게
        try: datetime.strptime(start_date_str, '%Y-%m-%d'); datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError: return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        print(f"Report requested for date range: {start_date_str} to {end_date_str}")

        selected_account_key = data.get('selected_account_key')
        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1: selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
            elif len(ACCOUNT_CONFIGS) > 1: return jsonify({"error": f"Meta 계정 키 필요. (사용 가능: {', '.join(ACCOUNT_CONFIGS.keys())})"}), 400
            else: return jsonify({"error": "설정된 Meta 광고 계정 없음."}), 400
        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config: return jsonify({"error": f"선택한 Meta 계정 키 '{selected_account_key}' 설정 없음."}), 404
        meta_account_id = account_config.get('id'); meta_token = account_config.get('token')
        if not meta_account_id or not meta_token: return jsonify({"error": f"Meta 계정 '{selected_account_key}' 설정 오류."}), 500
        meta_api_version = "v19.0" # Meta API 버전

        # --- 1. Cafe24 총계 데이터 가져오기 (최신 로직 유지, 토큰 오류 시 0 반환) ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0}; daily_cafe24_data = {"visitors": {}, "sales": {}}
        if 'cafe24_api' in globals() and CAFE24_CONFIGS:
            matching_cafe24_config = CAFE24_CONFIGS.get(selected_account_key)
            if matching_cafe24_config:
                print(f"Found matching Cafe24 config for '{selected_account_key}'. Attempting to fetch data...")
                try:
                    daily_cafe24_data = process_cafe24_data( selected_account_key, matching_cafe24_config, start_date_str, end_date_str )
                    if daily_cafe24_data and isinstance(daily_cafe24_data.get("visitors"), dict) and isinstance(daily_cafe24_data.get("sales"), dict):
                        total_visitors = sum(daily_cafe24_data["visitors"].values()); total_sales = sum(daily_cafe24_data["sales"].values())
                        cafe24_totals = {"total_visitors": total_visitors, "total_sales": int(round(total_sales))}
                        print(f"Cafe24 totals calculated. Visitors: {total_visitors}, Sales: {cafe24_totals['total_sales']}")
                    else: print("Warning: process_cafe24_data did not return expected structure.")
                except Exception as cafe24_err: print(f"Error during Cafe24 processing: {cafe24_err}"); traceback.print_exc()
            else: print(f"No matching Cafe24 config for '{selected_account_key}'.")
        else: print("Cafe24 module or configs not loaded. Skipping Cafe24 totals.")

        # --- 2. Meta 광고 데이터 가져오기 및 최종 보고서 생성 (fetch 함수는 최신, 내부 creative 함수는 복원+개선 버전 사용) ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        final_report_data = fetch_and_format_facebook_ads_data(
            start_date_str, end_date_str, meta_api_version, meta_account_id, meta_token,
            cafe24_totals # Cafe24 총계 전달
        )
        print("Meta Ads data fetch and report generation completed.")

        # --- 3. 결과 반환 ---
        print("--- Report generation process complete ---")
        return jsonify(final_report_data) # HTML 테이블과 JSON 데이터 포함

    except Exception as e: # 포괄적인 에러 처리
        error_message = "An internal server error occurred during report generation."
        print(f"{error_message} Details: {str(e)}"); traceback.print_exc()
        return jsonify({"error": error_message, "details": str(e)}), 500


# --- ★★★ 크리에이티브 관련 함수들 (사용자 제공 버전 기반 + target_url 개선 + fields 수정) ★★★ ---

def get_creative_details(ad_id, ver, token):
    """
    광고 ID -> 크리에이티브 ID -> 상세 정보 조회.
    이전 버전 로직 기반 + target_url 개선 + 오류 회피 위한 fields 수정.
    """
    creative_details = {'content_type': '알 수 없음', 'display_url': '', 'target_url': ''}
    try:
        # 1. Creative ID 얻기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params, timeout=10)
        creative_response.raise_for_status()
        creative_id = creative_response.json().get('creative', {}).get('id')

        if creative_id:
            # 2. Creative 상세 정보 얻기 (수정된 fields 사용)
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # 필드 요청 (Old Instagram ID 오류 회피 시도 + 필요한 링크 필드 포함)
            fields = 'object_type,image_url,thumbnail_url,video_id,object_story_spec{link_data{link,picture,image_url,video_id}},instagram_permalink_url,asset_feed_spec{videos{video_id,thumbnail_url},images{url},link_urls{website_url}}'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params, timeout=15)
            details_response.raise_for_status()
            details_data = details_response.json()

            # --- 디버깅 로그 (필요 시 주석 해제) ---
            # print(f"--- Creative RAW for ad_id: {ad_id} ---"); print(json.dumps(details_data, indent=2, ensure_ascii=False)); print("--- END RAW ---")
            # ---

            # 데이터 추출
            object_type = details_data.get('object_type')
            video_id = details_data.get('video_id')
            image_url = details_data.get('image_url')
            thumbnail_url = details_data.get('thumbnail_url')
            instagram_permalink_url = details_data.get('instagram_permalink_url')
            story_spec = details_data.get('object_story_spec', {})
            asset_feed_spec = details_data.get('asset_feed_spec', {})

            # AFS 데이터
            videos_from_feed = asset_feed_spec.get('videos', []) if asset_feed_spec else []
            feed_video_id = videos_from_feed[0].get('video_id') if videos_from_feed else None
            feed_thumbnail_url = videos_from_feed[0].get('thumbnail_url') if videos_from_feed else None
            images_from_feed = asset_feed_spec.get('images', []) if asset_feed_spec else []
            feed_image_url = images_from_feed[0].get('url') if images_from_feed else None
            link_urls_from_feed = asset_feed_spec.get('link_urls', []) if asset_feed_spec else []
            feed_website_url = link_urls_from_feed[0].get('website_url') if link_urls_from_feed else None # ★ AFS 랜딩 URL

            # OSS 데이터
            link_data = story_spec.get('link_data', {}) if story_spec else {}
            oss_image_url = link_data.get('picture') or link_data.get('image_url') # picture 우선
            oss_link = link_data.get('link') # ★ OSS 랜딩 URL
            oss_video_id = link_data.get('video_id')

            # 최종 값 결정
            actual_video_id = video_id or feed_video_id or oss_video_id
            # 표시 URL: 썸네일 > 피드 이미지 > 이미지 > OSS 이미지 순서로 찾기
            display_image_url = thumbnail_url or feed_thumbnail_url or image_url or feed_image_url or oss_image_url or ""
            # ★★ 대상 URL(target_url) 결정 로직: 실제 링크 우선! ★★
            best_target_url = feed_website_url or oss_link or instagram_permalink_url

            # 콘텐츠 유형 결정
            content_type = '알 수 없음' # 기본값
            if object_type == 'VIDEO' or actual_video_id:
                content_type = '동영상'
            elif object_type == 'PHOTO':
                 content_type = '사진'
            elif object_type == 'CAROUSEL' or (asset_feed_spec and (images_from_feed or videos_from_feed)):
                 content_type = '캐러셀'
            elif instagram_permalink_url:
                 content_type = '인스타그램'
            elif object_type == 'SHARE':
                 if actual_video_id: content_type = '동영상'
                 elif display_image_url: content_type = '사진' # 이미지 URL 있으면 사진으로 간주
                 else: content_type = '공유 게시물'
            elif display_image_url: # 유형 판별 안됐지만 이미지가 있으면 사진으로
                content_type = '사진'

            # 최종 값 할당
            creative_details['content_type'] = content_type
            creative_details['display_url'] = display_image_url

            # Target URL 최종 결정 (비디오 링크는 fallback)
            final_target_url = best_target_url
            if content_type == '동영상' and actual_video_id and not final_target_url:
                video_source_url = get_video_source_url(actual_video_id, ver, token)
                final_target_url = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={actual_video_id}"

            # 유효한 URL만 할당
            if isinstance(final_target_url, str) and final_target_url.startswith('http'):
                creative_details['target_url'] = final_target_url
            elif isinstance(display_image_url, str) and display_image_url.startswith('http') and content_type != '동영상':
                 # 동영상 아니면서 다른 링크 없고 표시 URL이 링크 형태면 표시 URL 사용
                 creative_details['target_url'] = display_image_url
            else:
                 creative_details['target_url'] = '' # 나머지는 빈 값

    # except 블록 (최신 유지)
    except requests.exceptions.Timeout: print(f"Timeout fetching creative details for ad_id {ad_id}.")
    except requests.exceptions.RequestException as e:
        response_text = e.response.text[:500] if hasattr(e, 'response') and e.response is not None else 'N/A'
        print(f"Error fetching creative details for ad_id {ad_id}: {e}. Response: {response_text}...")
    except Exception as e: print(f"Error processing creative details for ad_id {ad_id}: {e}")
    return creative_details

def get_video_source_url(video_id, ver, token):
    # 사용자가 제공한 이전 버전 로직 유지
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"; video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params, timeout=10); video_response.raise_for_status()
        return video_response.json().get('source')
    except Exception as e: print(f"Notice: Could not fetch video source for video {video_id}. Error: {e}"); return None

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    # 사용자가 제공한 이전 버전 로직 유지
    print(f"Fetching creative details for {len(ad_data)} ads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if not ad_data: print("No ad data for creatives."); return
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for i, future in enumerate(as_completed(futures)):
            ad_id = futures[future]; creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            try: creative_info = future.result()
            except Exception as e: print(f"Error in creative future for ad {ad_id}: {e}")
            if ad_id in ad_data: ad_data[ad_id]['creative_details'] = creative_info
    print("Finished fetching creative details.")


# --- ★★★ fetch_and_format_facebook_ads_data 함수 (최신 버전: Pagination, actions 처리, Cafe24 연동 등 포함) ★★★ ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals):
    # --- 이 함수는 페이지네이션, actions 처리, DataFrame 생성, HTML 테이블 생성 등 ---
    # --- 이전 답변에서 제공했던 최신 버전을 그대로 사용합니다. ---
    # --- 내부적으로 위에서 정의된 (복원+개선된) get_creative_details 함수를 사용합니다. ---

    all_records = []
    # actions 필드 상세 요청 (value 포함)
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions{action_type,value}'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = { 'fields': metrics, 'access_token': token, 'level': 'ad', 'time_range[since]': start_date, 'time_range[until]': end_date,
               'use_unified_attribution_setting': 'true', 'action_attribution_windows': ['1d_click', '7d_click', '1d_view'], 'limit': 200 }
    page_count = 1
    # 페이지네이션 처리
    while insights_url:
        print(f"Fetching Meta Ads data page {page_count}...")
        current_url = insights_url; current_params = params if page_count == 1 else {'access_token': token}
        try:
            response = requests.get(url=current_url, params=current_params, timeout=60); response.raise_for_status()
        except requests.exceptions.Timeout: print(f"Meta Ads API request timed out (Page: {page_count})."); break
        except requests.exceptions.RequestException as req_err: print(f"Meta Ads API network error (Page: {page_count}): {req_err}"); break
        data = response.json(); records_on_page = data.get('data', [])
        if not records_on_page: break
        all_records.extend(records_on_page)
        insights_url = data.get('paging', {}).get('next'); page_count += 1; params = None
    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")

    if not all_records:
        empty_html = "<p>선택한 기간에 Meta 광고 데이터가 없습니다.</p>"
        # Cafe24 총계만이라도 포함하는 기본 구조 반환
        return {"html_table": empty_html, "data": [], "cafe24_totals": cafe24_totals} # cafe24_totals 포함

    # 데이터 집계 (ad_id 기준) - actions 오류 수정된 로직 사용
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue
        if ad_id not in ad_data: # 초기화
            ad_data[ad_id] = {'ad_id': ad_id, 'ad_name': record.get('ad_name'), 'campaign_name': record.get('campaign_name'),
                              'adset_name': record.get('adset_name'), 'spend': 0.0, 'impressions': 0, 'link_clicks': 0, 'purchase_count': 0}
        # 값 누적
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except: pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except: pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0)) # 'clicks' 사용
        except: pass
        # 구매 수 집계 (actions 처리 개선 로직)
        actions_data = record.get('actions'); actions_list = []
        if isinstance(actions_data, dict): actions_list = actions_data.get('data', [])
        elif isinstance(actions_data, list): actions_list = actions_data
        if not isinstance(actions_list, list): actions_list = []
        purchase_count_on_record = 0
        for action in actions_list:
            if not isinstance(action, dict): continue
            action_type = action.get("action_type", "")
            # 다양한 구매 관련 action_type 처리
            if action_type in ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase", "website_purchase"]:
                try: value_str = action.get("value", "0"); purchase_count_on_record += int(float(value_str))
                except: pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record;
        # 이름 필드 업데이트
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # 크리에이티브 정보 병렬 조회 (위에서 복원/개선된 함수 호출)
    fetch_creatives_parallel(ad_data, ver, token)
    result_list = list(ad_data.values());
    if not result_list: return {"html_table": "<p>Meta 데이터 집계 결과 없음.</p>", "data": []}
    df = pd.DataFrame(result_list)

    # --- DataFrame 후처리 및 HTML/JSON 생성 (최신 로직 유지) ---
    # creative_details 컬럼 처리
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {'content_type': '알 수 없음', 'display_url': '', 'target_url': ''}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', '')) # 개선된 target_url 반영됨
    df = df.drop(columns=['creative_details'])

    # 숫자형 컬럼 처리 및 계산 지표 생성
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)
    df = df.rename(columns={'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명', 'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click', 'purchase_count': '구매 수'})
    int_cols = ['노출', 'Click', '구매 수', 'CPC', '구매당 비용'];
    for col in int_cols: df[col] = df[col].round(0).astype(int)
    df['FB 광고비용'] = df['FB 광고비용'].round(0).astype(int)

    # 합계 행 생성 (Cafe24 총계 포함)
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum(); total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0
    totals_data = {'광고명': '합계', '캠페인명': '', '광고세트명': '', 'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks, 'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases, '구매당 비용': total_cpp,
                   'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), 'Cafe24 매출': cafe24_totals.get('total_sales', 0),
                   'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''}
    totals_row = pd.Series(totals_data)
    df['ad_id'] = df['ad_id'] # ad_id 컬럼 존재 확인
    df['광고 성과'] = ''
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns and not df['ad_id'].isnull().all() else {}

    # 합계 행 추가 및 정렬
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce'); return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'], errors='ignore')

    # 광고 성과 분류 (최신: ad_id 기반 비교)
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy(); df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist() # 상위 3개 ad_id
    # ↓↓↓↓↓ 아래 함수 전체를 복사해서 붙여넣으세요 ↓↓↓↓↓
    def categorize_performance(row):
        # 합계 행은 제외
        if row['광고명'] == '합계':
            return ''

        ad_id_current = row.get('ad_id')
        # 구매당 비용을 숫자로 변환 (오류 시 무한대로 처리)
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')

        # 구매당 비용 계산 불가 시 제외 (NaN 또는 무한대 체크)
        # math 모듈 import 필요 -> 파일 상단에 'import math' 추가 확인
        if pd.isna(cost) or math.isinf(cost) or cost == 0:
            return ''

        # 기준 금액 이상이면 '개선 필요!'
        if cost >= 100000:
            return '개선 필요!'

        # 상위 ad_id 리스트(top_ad_ids)에 포함되는지 확인
        # 이 if 문의 들여쓰기가 'def' 보다 한 단계 안쪽이어야 함
        if ad_id_current in top_ad_ids:
            try:
                # 순위(인덱스) 찾기
                rank = top_ad_ids.index(ad_id_current)

                # 순위에 따라 성과 분류 반환 (들여쓰기 확인!)
                # 이 if/elif 블록은 try 블록 안에 있어야 함
                if rank == 0:
                    return '위닝 콘텐츠'
                elif rank == 1:
                    return '고성과 콘텐츠'
                elif rank == 2:
                    return '성과 콘텐츠'
                # rank 3 이상은 아래 최종 return '' 로 처리됨

            except ValueError:
                # 혹시 리스트에 없는 예외적인 경우
                return ''

        # 위 if문에 해당하지 않거나 rank 3 이상인 경우 빈칸 반환
        # 이 return 은 if ad_id_current... 와 같은 레벨이어야 함 (try 블록 밖)
        return ''
    # ↑↑↑↑↑ 이 함수 전체를 복사해서 붙여넣으세요 ↑↑↑↑↑
    if 'ad_id' in df_sorted.columns: df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    else: df_sorted['광고 성과'] = ''

    # URL 재매핑 (정렬 후)
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', '')) if 'ad_id' in df_sorted.columns else ''
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', '')) if 'ad_id' in df_sorted.columns else ''

    # --- HTML 테이블 생성 (최신 로직 유지) ---
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) and not isinstance(amount, str) else ("0 ₩" if pd.notna(amount) else "0 ₩")
    def format_number(num): return f"{int(num):,}" if pd.notna(num) and not isinstance(num, str) else ("0" if pd.notna(num) else "0")
    display_columns = ['광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', '구매 수', '구매당 비용', 'Cafe24 방문자 수', 'Cafe24 매출', '광고 성과', '콘텐츠 유형', '광고 콘텐츠']
    html_table = """<style> /* ... CSS ... */
        table { border-collapse: collapse; width: 100%; font-family: sans-serif; } th, td { border: 1px solid #ddd; padding: 8px; text-align: right; vertical-align: middle; } th { background-color: #f2f2f2; text-align: center; white-space: nowrap; } td.text-left { text-align: left; } td.text-center { text-align: center; } tr.total-row { font-weight: bold; background-color: #e9e9e9; } .ad-content-thumbnail { max-width: 60px; max-height: 60px; vertical-align: middle; } .ad-content-cell { width: 80px; text-align: center; } .winning-content { background-color: #d4edda; color: #155724; font-weight: bold; } .medium-performance { background-color: #fff3cd; color: #856404; } .third-performance { background-color: #e2e3e5; color: #383d41; } .needs-improvement { background-color: #f8d7da; color: #721c24; font-weight: bold; } a {text-decoration: none; color: inherit;} </style><table><thead><tr>"""
    for col_name in display_columns: html_table += f"<th>{col_name}</th>"
    html_table += "</tr></thead><tbody>"
    for index, row in df_sorted.iterrows():
        is_total_row = row.get('광고명') == '합계'; row_class = 'total-row' if is_total_row else ''
        html_table += f'<tr class="{row_class}">'
        for col in display_columns:
            value = None; td_class = []; td_align = 'right'
            # 컬럼별 처리 ...
            if col in ['광고명', '캠페인명', '광고세트명']: value = row.get(col, ''); td_align = 'left'; td_class.append('text-left')
            elif col in ['FB 광고비용', 'CPC', '구매당 비용', 'Cafe24 매출']: value = format_currency(row.get(col))
            elif col in ['노출', 'Click', '구매 수', 'Cafe24 방문자 수']: value = format_number(row.get(col))
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
            elif col == '광고 콘텐츠': # ★ 개선된 target_url 적용 ★
                display_url = row.get('display_url', ''); target_url = row.get('target_url', '')
                content_tag = "";
                if not is_total_row and display_url:
                    img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
                    if isinstance(target_url, str) and target_url.startswith('http'): content_tag = f'<a href="{target_url}" target="_blank" title="콘텐츠 보기">{img_tag}</a>'
                    else: content_tag = img_tag
                elif not is_total_row: content_tag = "-"
                value = content_tag; td_class.append("ad-content-cell"); td_align = 'center'
            else: value = row.get(col, '')
            if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'
            td_style = f'text-align: {td_align};'; td_class_attr = f' class="{" ".join(td_class)}"' if td_class else ''
            html_table += f'<td{td_class_attr} style="{td_style}">{value}</td>'
        html_table += "</tr>\n"
    html_table += "</tbody></table>"

    # --- JSON 데이터 준비 (최신 버전 로직 유지) ---
    final_columns_for_json = [col for col in display_columns if col not in ['광고 콘텐츠']] + ['ad_id']
    df_for_json = df_sorted[[col for col in final_columns_for_json if col in df_sorted.columns]].copy()
    def clean_data_for_json(obj): # 클리닝 함수
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
    records = df_for_json.to_dict(orient='records'); cleaned_records = clean_data_for_json(records)
    return {"html_table": html_table, "data": cleaned_records}

# --- 앱 실행 부분 (최신 유지) ---
# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 5001)))
