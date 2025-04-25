# api/index.py
# -*- coding: utf-8 -*-
import math
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
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if not os.path.exists(dotenv_path): dotenv_path = '.env'
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path); print(f"dotenv loaded from {os.path.abspath(dotenv_path)}.")
    else: print(f"dotenv file not found in {os.path.dirname(__file__)} or parent directory, skipping .env load.")
except ImportError: print("dotenv not installed, skipping .env load."); pass

# --- Flask 앱 인스턴스 생성 ---
app = Flask(__name__)

# --- Meta 계정 설정 로드 ---
def load_account_configs():
    accounts = {}; i = 1
    while True:
        name = os.environ.get(f"ACCOUNT_CONFIG_{i}_NAME")
        account_id = os.environ.get(f"ACCOUNT_CONFIG_{i}_ID")
        token = os.environ.get(f"ACCOUNT_CONFIG_{i}_TOKEN")
        if name and account_id and token:
            name = name.strip(); account_id = account_id.strip(); token = token.strip()
            if name and account_id and token:
                accounts[name] = {"id": account_id, "token": token, "name": name}
                print(f"Loaded Meta account: {name} (ID: {account_id})"); i += 1
            else: print(f"Warning: Skipped Meta account config index {i} due to empty value after stripping."); i += 1
        else:
            if name or account_id or token: print(f"Warning: Incomplete Meta account config index {i}. Check name, id, token.")
            break
    if not accounts: print("CRITICAL: No complete Meta account configurations found.")
    return accounts
ACCOUNT_CONFIGS = load_account_configs()

# --- Cafe24 계정 설정 로드 ---
# 사용자가 제공한 코드에 있던 Cafe24 설정 로드 함수 (환경 변수 기반)
def load_cafe24_configs_from_env():
    configs = {}
    i = 1
    while True:
        meta_account_name = os.environ.get(f"ACCOUNT_CONFIG_{i}_NAME")
        if not meta_account_name: break

        mall_id = os.environ.get(f"CAFE24_CONFIG_{i}_MALL_ID")
        client_id = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_ID")
        client_secret = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_SECRET")
        # access_token = os.environ.get(f"CAFE24_CONFIG_{i}_ACCESS_TOKEN") # 이제 사용 안 함
        refresh_token = os.environ.get(f"CAFE24_CONFIG_{i}_REFRESH_TOKEN") # Refresh Token 사용

        # 필수 정보 (Refresh Token 포함) 확인
        if mall_id and client_id and client_secret and refresh_token:
              meta_account_name = meta_account_name.strip(); mall_id = mall_id.strip(); client_id = client_id.strip(); client_secret = client_secret.strip(); refresh_token = refresh_token.strip()
              if meta_account_name and mall_id and client_id and client_secret and refresh_token:
                   configs[meta_account_name] = {"mall_id": mall_id, "client_id": client_id, "client_secret": client_secret, "refresh_token": refresh_token}
                   print(f"Loaded Cafe24 config for Meta account: {meta_account_name} (Mall ID: {mall_id})")
              else: print(f"Warning: Skipped Cafe24 config for Meta account index {i} due to empty value after stripping.")
        elif mall_id or client_id or client_secret or refresh_token:
             print(f"Warning: Incomplete Cafe24 config for Meta account index {i} ('{meta_account_name}'). Missing required fields (MALL_ID, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN).")
        i += 1
    if not configs: print("Notice: No complete Cafe24 configurations found.")
    return configs

# Cafe24 설정 로드 실행
if 'cafe24_api' in globals(): CAFE24_CONFIGS = load_cafe24_configs_from_env()
else: print("Warning: Cafe24 module not loaded, skipping Cafe24 config loading."); CAFE24_CONFIGS = {}

# CORS 허용 설정
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response

# API 라우트 정의
@app.route('/', methods=['GET'])
@app.route('/api', methods=['GET'])
def home(): return jsonify({"message": "Facebook & Cafe24 Ads Report API is running."})

@app.route('/api/accounts', methods=['POST', 'OPTIONS'])
def get_accounts():
    # ... (이전과 동일) ...
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json();
        if not data: return jsonify({"error": "JSON body required."}), 400
        password = data.get('password'); report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password): return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403
        account_names = list(ACCOUNT_CONFIGS.keys());
        if not account_names: print("Warning: No Meta accounts configured, returning empty list.")
        return jsonify(account_names)
    except Exception as e: print(f"Error getting account list: {e}"); traceback.print_exc(); return jsonify({"error": "Failed to retrieve account list."}), 500

@app.route('/api/generate-report', methods=['POST', 'OPTIONS'])
def generate_report():
    # ... (이전과 동일) ...
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json();
        if not data: return jsonify({"error": "JSON body required."}), 400
        password = data.get('password'); report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password): return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        today = datetime.now().date(); default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date_str = data.get('start_date') or default_date; end_date_str = data.get('end_date') or start_date_str
        try: datetime.strptime(start_date_str, '%Y-%m-%d'); datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError: return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400
        print(f"Report requested for date range: {start_date_str} to {end_date_str}")

        selected_account_key = data.get('selected_account_key')
        if not ACCOUNT_CONFIGS: return jsonify({"error": "설정된 Meta 광고 계정이 없습니다."}), 500
        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1: selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]; print(f"Only one Meta account found, auto selected: '{selected_account_key}'")
            else: return jsonify({"error": f"Meta 계정을 선택해주세요. 사용 가능: {', '.join(ACCOUNT_CONFIGS.keys())}"}), 400
        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config: return jsonify({"error": f"선택한 Meta 계정 키 '{selected_account_key}' 설정 없음."}), 404
        meta_account_id = account_config.get('id'); meta_token = account_config.get('token')
        if not meta_account_id or not meta_token: print(f"Error: Incomplete config for Meta account '{selected_account_key}'."); return jsonify({"error": f"Meta 계정 '{selected_account_key}' 설정 오류."}), 500

        meta_api_version = "v19.0"
        print(f"Using Meta Account: '{selected_account_key}' (ID: {meta_account_id}), API Version: {meta_api_version}")

        # --- Cafe24 데이터 가져오기 (오류 시 0 반환) ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0}
        if 'cafe24_api' in globals() and CAFE24_CONFIGS and selected_account_key in CAFE24_CONFIGS:
            matching_cafe24_config = CAFE24_CONFIGS[selected_account_key]
            print(f"Found matching Cafe24 config. Fetching Cafe24 data...")
            try:
                daily_cafe24_data = process_cafe24_data(selected_account_key, matching_cafe24_config, start_date_str, end_date_str)
                if daily_cafe24_data and isinstance(daily_cafe24_data.get("visitors"), dict) and isinstance(daily_cafe24_data.get("sales"), dict):
                    total_visitors = sum(daily_cafe24_data["visitors"].values()); total_sales = sum(daily_cafe24_data["sales"].values())
                    cafe24_totals = {"total_visitors": int(round(total_visitors)), "total_sales": int(round(total_sales))}
                    print(f"Cafe24 totals calculated. Visitors: {cafe24_totals['total_visitors']}, Sales: {cafe24_totals['total_sales']}")
                else: print("Warning: process_cafe24_data returned unexpected structure.")
            except Exception as cafe24_err: print(f"Error during Cafe24 processing: {cafe24_err}"); traceback.print_exc()
        else: print("Notice: Skipping Cafe24 data fetch (module/config not found).")

        # --- Meta 데이터 가져오기 및 보고서 생성 ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        final_report_data = fetch_and_format_facebook_ads_data(
            start_date_str, end_date_str, meta_api_version, meta_account_id, meta_token,
            cafe24_totals
        )
        print("Meta Ads data fetch and report formatting completed.")

        # --- 결과 반환 ---
        print("--- Report generation process complete ---")
        return jsonify(final_report_data)

    except Exception as e:
        error_message = "An internal server error occurred."
        print(f"{error_message} Details: {str(e)}"); traceback.print_exc()
        return jsonify({"error": error_message, "details": str(e)}), 500

# --- ★★★ 크리에이티브 함수 최종 수정 (단순화 + 광고 소재 링크) ★★★ ---

def get_creative_details(ad_id, ver, token):
    """
    광고 ID -> 크리에이티브 ID -> 상세 정보 조회.
    콘텐츠 유형 '동영상'/'사진' 단순화, 클릭 시 광고 소재 링크 연결 시도.
    """
    # 기본 반환 구조 + creative_asset_url 추가
    creative_details = {
        'content_type': '',        # 최종 유형: 동영상 or 사진 or 기타
        'display_url': '',         # HTML 테이블 표시용 썸네일
        'target_url': '',          # 랜딩 페이지 URL (참고용)
        'creative_asset_url': '' # ★ 썸네일 클릭 시 열릴 광고 소재(동영상/사진) URL ★
    }
    creative_id = None
    try:
        # 1. Creative ID 얻기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params, timeout=10)
        creative_response.raise_for_status()
        creative_id = creative_response.json().get('creative', {}).get('id')

        if creative_id:
            # 2. Creative 상세 정보 얻기 (오류 회피 + 필요 필드)
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # video_id 포함, link 포함 필드 요청
            fields = ('object_type,image_url,thumbnail_url,video_id,'
                      'object_story_spec{link_data{link,picture,image_url,video_id}},'
                      'asset_feed_spec{videos{video_id,thumbnail_url},images{url},link_urls{website_url}},'
                      'instagram_permalink_url') # effective_object_story_id 등 제거
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params, timeout=15)
            details_response.raise_for_status()
            details_data = details_response.json()

            # 3. 데이터 추출
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

            # 4. 최종 값 결정
            actual_video_id = video_id or feed_video_id or oss_video_id
            display_image_url_options = [thumbnail_url, feed_thumbnail_url, image_url, feed_image_url, oss_picture_url, oss_image_url]
            display_image_url = next((url for url in display_image_url_options if isinstance(url, str) and url.startswith('http')), '')
            landing_page_url = feed_website_url or oss_link # 랜딩 페이지 URL

            # ★ 콘텐츠 유형 단순화: 동영상 또는 사진 ★
            content_type = '기타' # 기본값
            if actual_video_id or (asset_feed_spec and videos_from_feed):
                 content_type = '동영상'
            elif display_image_url: # 이미지 URL 있으면 무조건 사진
                 content_type = '사진'
            elif instagram_permalink_url: # 인스타 링크만 있으면 일단 사진으로? (개선 여지 있음)
                content_type = '사진'

            creative_details['content_type'] = content_type
            creative_details['display_url'] = display_image_url
            creative_details['target_url'] = landing_page_url if isinstance(landing_page_url, str) and landing_page_url.startswith('http') else '' # 참고용 랜딩 페이지

            # ★ 광고 소재 URL (클릭 시 이동할 곳) ★
            creative_asset_url = ''
            if content_type == '동영상' and actual_video_id:
                video_source_url = get_video_source_url(actual_video_id, ver, token)
                creative_asset_url = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={actual_video_id}"
            elif content_type == '사진':
                # 사진일 경우, 고화질 이미지 URL 우선 사용
                image_options = [image_url, feed_image_url, oss_image_url, oss_picture_url, thumbnail_url, feed_thumbnail_url]
                best_image_for_asset = next((url for url in image_options if isinstance(url, str) and url.startswith('http')), '')
                creative_asset_url = best_image_for_asset
            elif instagram_permalink_url: # 유형이 '사진'/'동영상'아닌데 인스타 링크 있으면 사용
                 creative_asset_url = instagram_permalink_url

            creative_details['creative_asset_url'] = creative_asset_url if isinstance(creative_asset_url, str) and creative_asset_url.startswith('http') else ''

        else: print(f"Warning: Could not get Creative ID for Ad ID: {ad_id}")

    except requests.exceptions.RequestException as e:
        response_text = e.response.text[:500] if hasattr(e, 'response') and e.response is not None else 'N/A'
        print(f"Warning: Network error for Ad ID {ad_id}. Error: {e}. Response: {response_text}...")
    except Exception as e: print(f"Warning: Processing error for Ad ID {ad_id}. Error: {e}")
    return creative_details

def get_video_source_url(video_id, ver, token):
    # 이전 버전 유지
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"; video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params, timeout=10); video_response.raise_for_status()
        source_url = video_response.json().get('source')
        return source_url if isinstance(source_url, str) and source_url.startswith('http') else None
    except Exception: return None # 오류 시 조용히 None 반환

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    # 이전 버전 유지 (안정적)
    print(f"Fetching creative details for {len(ad_data)} ads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if not ad_data: print("No ad data for creatives."); return
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for i, future in enumerate(as_completed(futures)):
            ad_id = futures[future]; creative_info = {'content_type': '오류', 'display_url': '', 'target_url': '', 'creative_asset_url': ''} # 기본값
            try: creative_info = future.result()
            except Exception as e: print(f"Error in creative future for ad {ad_id}: {e}")
            if ad_id in ad_data: ad_data[ad_id]['creative_details'] = creative_info
    print("Finished fetching creative details.")

# --- ★★★ fetch_and_format_facebook_ads_data 함수 (최신 버전 + HTML 수정) ★★★ ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals):
    # ... (데이터 가져오기 및 집계 로직은 이전과 동일) ...
    all_records = []; metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions{action_type,value}'; insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = { 'fields': metrics, 'access_token': token, 'level': 'ad', 'time_range[since]': start_date, 'time_range[until]': end_date, 'use_unified_attribution_setting': 'true', 'action_attribution_windows': ['1d_click', '7d_click', '1d_view'], 'limit': 200 }; page_count = 1
    while insights_url: # 페이지네이션
        print(f"Fetching Meta Ads data page {page_count}..."); current_url = insights_url; current_params = params if page_count == 1 else {'access_token': token}
        try: response = requests.get(url=current_url, params=current_params, timeout=60); response.raise_for_status()
        except requests.exceptions.Timeout: print(f"Meta Ads API request timed out (Page: {page_count})."); break
        except requests.exceptions.RequestException as req_err: print(f"Meta Ads API network error (Page: {page_count}): {req_err}"); break
        data = response.json(); records_on_page = data.get('data', []);
        if not records_on_page: break
        all_records.extend(records_on_page); insights_url = data.get('paging', {}).get('next'); page_count += 1; params = None
    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records: return {"html_table": "<p>선택한 기간에 Meta 광고 데이터가 없습니다.</p>", "data": [], "cafe24_totals": cafe24_totals}

    # 데이터 집계 (최신)
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue
        if ad_id not in ad_data: ad_data[ad_id] = {'ad_id': ad_id, 'ad_name': record.get('ad_name'), 'campaign_name': record.get('campaign_name'), 'adset_name': record.get('adset_name'), 'spend': 0.0, 'impressions': 0, 'link_clicks': 0, 'purchase_count': 0}
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except: pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except: pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0))
        except: pass
        actions_data = record.get('actions'); actions_list = [];
        if isinstance(actions_data, dict): actions_list = actions_data.get('data', [])
        elif isinstance(actions_data, list): actions_list = actions_data
        if not isinstance(actions_list, list): actions_list = []
        purchase_count_on_record = 0
        for action in actions_list:
            if not isinstance(action, dict): continue
            action_type = action.get("action_type", "");
            if action_type in ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase", "website_purchase"]:
                try: value_str = action.get("value", "0"); purchase_count_on_record += int(float(value_str))
                except: pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record;
        if record.get('ad_name'): ad_data[ad_id]['ad_name'] = record.get('ad_name')
        if record.get('campaign_name'): ad_data[ad_id]['campaign_name'] = record.get('campaign_name')
        if record.get('adset_name'): ad_data[ad_id]['adset_name'] = record.get('adset_name')

    # 크리에이티브 정보 병렬 조회 (수정된 함수 호출)
    fetch_creatives_parallel(ad_data, ver, token)
    result_list = list(ad_data.values());
    if not result_list: return {"html_table": "<p>Meta 데이터 집계 결과 없음.</p>", "data": []}
    df = pd.DataFrame(result_list)

    # --- DataFrame 후처리 (creative_asset_url 포함) ---
    default_creative_details = {'content_type': '알 수 없음', 'display_url': '', 'target_url': '', 'creative_asset_url': ''}
    df['creative_details_dict'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', default_creative_details))
    df['콘텐츠 유형'] = df['creative_details_dict'].apply(lambda x: x.get('content_type', '알 수 없음')) # 단순화된 유형 적용됨
    df['display_url'] = df['creative_details_dict'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details_dict'].apply(lambda x: x.get('target_url', '')) # 랜딩 페이지 URL
    df['creative_asset_url'] = df['creative_details_dict'].apply(lambda x: x.get('creative_asset_url', '')) # ★ 소재 URL
    df = df.drop(columns=['creative_details_dict'])

    # --- 나머지 후처리 및 정렬 (기존 코드 유지) ---
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)
    df = df.rename(columns={'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명', 'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click', 'purchase_count': '구매 수'})
    int_cols = ['노출', 'Click', '구매 수', 'CPC', '구매당 비용'];
    for col in int_cols: df[col] = df[col].round(0).astype(int)
    df['FB 광고비용'] = df['FB 광고비용'].round(0).astype(int)
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum(); total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0
    totals_data = {'광고명': '합계', '캠페인명': '', '광고세트명': '', 'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks, 'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases, '구매당 비용': total_cpp,
                   'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), 'Cafe24 매출': cafe24_totals.get('total_sales', 0),
                   'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', 'creative_asset_url': '', '광고 성과': ''}
    totals_row = pd.Series(totals_data)
    if 'ad_id' not in df.columns: df['ad_id'] = None
    else: df['ad_id'] = df['ad_id']
    df['광고 성과'] = ''
    df_valid_ad_id = df.dropna(subset=['ad_id'])
    # ★ url_map 에 creative_asset_url 추가 ★
    url_map = df_valid_ad_id.set_index('ad_id')[['display_url', 'target_url', 'creative_asset_url']].to_dict('index') if not df_valid_ad_id.empty else {}
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce'); return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'], errors='ignore')
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy(); df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()

    # ★ categorize_performance 함수 (SyntaxError 수정된 버전) ★
    def categorize_performance(row):
        if row['광고명'] == '합계': return ''
        ad_id_current = row.get('ad_id')
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        if pd.isna(cost) or math.isinf(cost) or cost == 0: return ''
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

    # --- URL 재매핑 (정렬 후) ---
    # ★★★ creative_asset_url 포함 ★★★
    if 'ad_id' in df_sorted.columns:
        df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', ''))
        df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', ''))
        df_sorted['creative_asset_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('creative_asset_url', ''))
    else:
        df_sorted['display_url'] = ''
        df_sorted['target_url'] = ''
        df_sorted['creative_asset_url'] = ''


    # --- ★★★ HTML 테이블 생성 (creative_asset_url 사용하도록 수정) ★★★ ---
    print("Generating HTML table...")
    def format_currency(amount):
        try: return f"{int(amount):,} ₩"
        except (ValueError, TypeError): return "0 ₩"
    def format_number(num):
        try: return f"{int(num):,}"
        except (ValueError, TypeError): return "0"

    display_columns = ['광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', '구매 수', '구매당 비용', 'Cafe24 방문자 수', 'Cafe24 매출', '광고 성과', '콘텐츠 유형', '광고 콘텐츠']
    # CSS 스타일... (이전과 동일)
    html_table = """<style> /* ... CSS는 이전 답변 내용 참고 ... */ </style><table><thead><tr>"""
    for col_name in display_columns: html_table += f"<th>{col_name}</th>"
    html_table += "</tr></thead><tbody>"

    for index, row in df_sorted.iterrows():
        is_total_row = row.get('광고명') == '합계'; row_class = 'total-row' if is_total_row else ''
        html_table += f'<tr class="{row_class}">'
        for col in display_columns:
            value = None; td_class = []; td_align = 'right' # 기본값

            # 컬럼별 값 처리
            if col in ['광고명', '캠페인명', '광고세트명']: value = row.get(col, ''); td_align = 'left'; td_class.append('text-left')
            elif col in ['FB 광고비용', 'CPC', '구매당 비용', 'Cafe24 매출']: value = format_currency(row.get(col))
            elif col in ['노출', 'Click', '구매 수', 'Cafe24 방문자 수']: value = format_number(row.get(col))
            elif col == 'CTR': value = row.get(col, '0.00%')
            elif col == '광고 성과':
                performance_text = row.get(col, ''); performance_class = '';
                if performance_text == '위닝 콘텐츠':
                    performance_class = 'winning-content'
                elif performance_text == '고성과 콘텐츠':
                    performance_class = 'medium-performance'
                elif performance_text == '성과 콘텐츠':
                    performance_class = 'third-performance'
                elif performance_text == '개선 필요!':
                    performance_class = 'needs-improvement'
                value = performance_text;
                if performance_class: td_class.append(performance_class)
                td_align = 'center'; td_class.append('text-center')
            elif col == '콘텐츠 유형': value = row.get(col, '-') if not is_total_row else ''; td_align = 'center'; td_class.append('text-center')
            elif col == '광고 콘텐츠':
                # ★★★ creative_asset_url을 링크로 사용 ★★★
                display_url = row.get('display_url', '')
                creative_asset_url = row.get('creative_asset_url', '') # 소재 자체 링크
                content_tag = ""
                if not is_total_row and display_url:
                    img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
                    # creative_asset_url이 유효하면 링크 생성
                    if isinstance(creative_asset_url, str) and creative_asset_url.startswith('http'):
                        content_tag = f'<a href="{creative_asset_url}" target="_blank" title="광고 소재 보기">{img_tag}</a>'
                    else: content_tag = img_tag # 소재 링크 없으면 이미지만 표시
                elif not is_total_row: content_tag = "-"
                value = content_tag; td_class.append("ad-content-cell"); td_align = 'center'
            else: value = row.get(col, '')

            # 합계행 아니면 Cafe24 값 '-' 처리
            if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'

            # 최종 셀 생성
            td_style = f'text-align: {td_align};'
            td_class_attr = f' class="{" ".join(td_class)}"' if td_class else ''
            html_table += f'<td{td_class_attr} style="{td_style}">{value}</td>'
        html_table += "</tr>\n"
    html_table += "</tbody></table>"
    print("HTML table generated.")

    # --- JSON 데이터 준비 ---
    # (JSON 생성 및 클리닝 로직은 이전과 동일하게 유지)
    print("Preparing JSON data...")
    final_columns_for_json = [col for col in display_columns if col not in ['광고 콘텐츠']] + ['ad_id', 'display_url', 'target_url', 'creative_asset_url'] # URL 정보 포함
    df_for_json = df_sorted[[col for col in final_columns_for_json if col in df_sorted.columns]].copy()
    def clean_data_for_json(obj):
        # (클리닝 함수 내용은 이전과 동일)
        if isinstance(obj, dict): return {k: clean_data_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [clean_data_for_json(elem) for elem in obj]
        elif isinstance(obj, (int, float)):
            if pd.isna(obj) or math.isinf(obj): return None
            if hasattr(obj, 'item'): return obj.item()
            return obj
        elif isinstance(obj, (pd.Timestamp, date)): return obj.isoformat()
        elif pd.isna(obj): return None
        elif hasattr(obj, 'item'):
             try: return obj.item()
             except: return str(obj)
        elif isinstance(obj, (bool, str)) or obj is None: return obj
        else: return str(obj)
    records = df_for_json.to_dict(orient='records'); cleaned_records = clean_data_for_json(records)
    print("JSON data prepared.")
    return {"html_table": html_table, "data": cleaned_records}

# --- 앱 실행 부분 ---
# (이전과 동일)
# if __name__ == '__main__':
#     # ... (로컬 실행 코드) ...
