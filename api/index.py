# api/index.py
# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from flask import Flask, jsonify, request
import re

# --- Cafe24 API 모듈 import ---
# 같은 api 폴더 내의 cafe24_api.py 에서 설정과 함수를 가져옴
from .cafe24_api import CAFE24_CONFIGS, process_cafe24_data

# .env 파일 로드 (Vercel 등에서는 환경 변수로 직접 설정)
# from dotenv import load_dotenv
# load_dotenv()

app = Flask(__name__)

# --- Meta 계정 설정 로드 ---
def load_account_configs():
    """환경 변수에서 여러 Meta 계정 설정을 로드합니다."""
    accounts = {}
    i = 1
    while True:
        name_key = f"ACCOUNT_CONFIG_{i}_NAME"
        id_key = f"ACCOUNT_CONFIG_{i}_ID"
        token_key = f"ACCOUNT_CONFIG_{i}_TOKEN"
        name = os.environ.get(name_key)
        account_id = os.environ.get(id_key)
        token = os.environ.get(token_key)

        # 이름, ID, 토큰이 모두 있어야 유효한 설정으로 간주
        if name and account_id and token:
            account_key = name # 설정을 구분하는 키로 NAME 사용
            accounts[account_key] = {"id": account_id, "token": token, "name": name}
            print(f"Loaded Meta account: {name} (ID: {account_id})")
            i += 1
        else:
             # 첫 번째 설정의 이름조차 없으면 바로 중단
             if i == 1 and not name: pass
             # 일부만 있는 경우 경고
             elif name or account_id or token:
                 print(f"Warning: Incomplete Meta account configuration found for index {i}. Skipping.")
             break # 다음 인덱스로 넘어가지 않음
    if not accounts:
        print("Warning: No complete Meta account configurations found in environment variables (e.g., ACCOUNT_CONFIG_1_NAME/ID/TOKEN).")
    return accounts

ACCOUNT_CONFIGS = load_account_configs()
# --- Meta 계정 설정 로드 끝 ---


# CORS 허용
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/api', methods=['GET'])
def home():
    """API 기본 경로"""
    return jsonify({"message": "Facebook & Cafe24 광고 성과 보고서 API"})

# --- Meta 계정 목록 제공 API ---
@app.route('/api/accounts', methods=['POST'])
def get_accounts():
    """Meta 광고 계정 이름 목록을 반환합니다."""
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        # 비밀번호 환경 변수가 설정되어 있는지 확인
        report_password = os.environ.get("REPORT_PASSWORD")
        if not report_password:
             print("Warning: REPORT_PASSWORD environment variable is not set.")
             # 비밀번호가 설정되지 않았을 경우 어떻게 처리할지 결정 (예: 접근 허용 또는 오류 반환)
             # 여기서는 일단 통과시키지만, 보안상 설정하는 것이 좋습니다.
             pass
        elif not password or password != report_password:
             return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 로드된 Meta 계정 설정의 이름 목록 반환
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)
    except Exception as e:
        print(f"Error getting account list: {e}")
        traceback.print_exc() # 상세 오류 로깅
        return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API ---
@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    """선택된 Meta 계정의 광고 데이터와 해당하는 Cafe24 데이터를 가져와 보고서를 생성합니다."""
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
             return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 날짜 처리
        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date
        print(f"Report requested for date range: {start_date} to {end_date}")

        # Meta 계정 선택
        selected_account_key = data.get('selected_account_key')
        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1:
                selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
                print(f"No selected_account_key provided, defaulting to: {selected_account_key}")
            elif len(ACCOUNT_CONFIGS) > 1:
                 return jsonify({"error": f"여러 개의 Meta 계정이 설정되어 있습니다. 'selected_account_key'를 지정해주세요. (사용 가능: {', '.join(ACCOUNT_CONFIGS.keys())})"}), 400
            else: # 설정된 계정이 없을 때
                 return jsonify({"error": "설정된 Meta 광고 계정이 없습니다. 환경 변수를 확인하세요."}), 400

        # 선택된 Meta 계정 정보 가져오기
        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config:
            return jsonify({"error": f"선택한 Meta 계정 키 '{selected_account_key}'에 대한 설정을 찾을 수 없습니다. (사용 가능: {', '.join(ACCOUNT_CONFIGS.keys())})"}), 404

        meta_account_id = account_config.get('id')
        meta_token = account_config.get('token')
        if not meta_account_id or not meta_token:
             print(f"Error: Missing ID or Token for Meta account key '{selected_account_key}'.")
             return jsonify({"error": "서버 설정 오류: Meta 계정 정보가 완전하지 않습니다."}), 500

        meta_api_version = "v19.0" # Meta API 버전

        # --- 1. Meta 광고 데이터 가져오기 ---
        print(f"Fetching Meta Ads data for account: {selected_account_key} (ID: {meta_account_id})...")
        meta_result = fetch_and_format_facebook_ads_data(start_date, end_date, meta_api_version, meta_account_id, meta_token)
        print("Meta Ads data fetch completed.")

        # --- 2. Cafe24 데이터 가져오기 ---
        cafe24_daily_data = {"visitors": {}, "sales": {}} # 기본값 초기화
        # 선택된 Meta 계정 키와 동일한 이름의 Cafe24 설정 찾기
        selected_cafe24_config = CAFE24_CONFIGS.get(selected_account_key)

        if selected_cafe24_config:
            print(f"Found matching Cafe24 config for '{selected_account_key}'. Fetching Cafe24 data...")
            # cafe24_api 모듈의 함수 호출 (설정 키와 설정 딕셔너리 전달)
            cafe24_daily_data = process_cafe24_data(selected_account_key, selected_cafe24_config, start_date, end_date)
            print("Cafe24 data fetch attempted.")
        else:
            print(f"Cafe24 configuration not found for the key '{selected_account_key}'. Skipping Cafe24 data fetch.")
            # 이 경우 cafe24_daily_data는 초기값인 빈 딕셔너리로 유지됨

        # --- 3. 최종 결과 조합 ---
        final_result = {
            "meta_report": meta_result, # {"html_table": "...", "data": [...]}
            "cafe24_daily_data": cafe24_daily_data # {"visitors": {...}, "sales": {...}}
        }
        print("--- Report generation complete ---")
        return jsonify(final_result)

    # --- 오류 처리 ---
    except requests.exceptions.RequestException as req_err:
        error_message = f"API request failed: {str(req_err)}"
        print(error_message)
        traceback.print_exc()
        return jsonify({"error": error_message}), 500
    except KeyError as key_err:
        error_message = f"Error processing API data (missing key): {str(key_err)}"
        print(error_message)
        traceback.print_exc()
        return jsonify({"error": error_message}), 500
    except Exception as e:
        error_message = "An internal server error occurred."
        print(f"{error_message} Details: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": error_message}), 500


# --- 메타 광고 크리에이티브 관련 함수들 ---
def get_creative_details(ad_id, ver, token):
    """광고 ID를 기반으로 크리에이티브 상세 정보(유형, 썸네일, 타겟 URL)를 가져옵니다."""
    creative_details = {'content_type': '알 수 없음', 'display_url': '', 'target_url': ''}
    try:
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token} # creative id 만 요청
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_id = creative_response.json().get('creative', {}).get('id')

        if creative_id:
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # 필요한 필드들을 효율적으로 요청
            fields = 'object_type,image_url,thumbnail_url,video_id,effective_object_story_id,object_story_spec{link_data{link,picture,image_hash,image_url,video_id}},instagram_permalink_url,asset_feed_spec{videos{video_id,thumbnail_url}}'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params)
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

            # 콘텐츠 유형 및 URL 결정 로직 (이전과 동일)
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
                creative_details['target_url'] = oss_link or creative_details['display_url'] # 링크가 있으면 링크 우선
            elif object_type == 'SHARE': # SHARE 타입 처리
                if videos_from_feed or oss_video_id:
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = feed_thumbnail_url or thumbnail_url or image_url or oss_image_url or ""
                     share_video_id = feed_video_id or oss_video_id
                     video_source_url = get_video_source_url(share_video_id, ver, token)
                     creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={share_video_id}"
                elif link_data and (link_data.get('image_hash') or oss_image_url):
                     creative_details['content_type'] = '사진'
                     creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                     creative_details['target_url'] = oss_link or creative_details['display_url']
                elif instagram_permalink_url:
                     creative_details['content_type'] = '인스타그램' # 좀 더 명확하게
                     creative_details['display_url'] = thumbnail_url or image_url or ""
                     creative_details['target_url'] = instagram_permalink_url
                elif thumbnail_url: # 썸네일만 있는 경우 (비디오 추정)
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = thumbnail_url
                     story_id = details_data.get('effective_object_story_id')
                     creative_details['target_url'] = f"https://www.facebook.com/{story_id}" if story_id and "_" in story_id else thumbnail_url
                else: # 기타 SHARE
                     creative_details['content_type'] = '공유 게시물'
                     creative_details['display_url'] = image_url or thumbnail_url or ""
                     creative_details['target_url'] = oss_link or creative_details['display_url']
            elif thumbnail_url: # 타입 불명확 시 썸네일 기반으로 사진 처리
                 creative_details['content_type'] = '사진'
                 creative_details['display_url'] = thumbnail_url
                 creative_details['target_url'] = creative_details['display_url']

    except requests.exceptions.RequestException as e: print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e: print(f"Error processing creative details for ad {ad_id}: {e}")
    return creative_details

def get_video_source_url(video_id, ver, token):
    """비디오 ID로 실제 비디오 소스 URL을 가져옵니다 (권한 필요)."""
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params, timeout=10) # 타임아웃 추가
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except requests.exceptions.Timeout:
        print(f"Timeout fetching video source for video {video_id}.")
        return None
    except Exception as e:
        # 오류는 로깅하되, 소스 URL을 못 가져와도 진행은 되도록 None 반환
        # print(f"Notice: Could not fetch video source for video {video_id}. Might lack permissions or video is private. Error: {e}")
        return None

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    """여러 광고의 크리에이티브 정보를 병렬로 가져옵니다."""
    print(f"Fetching creative details for {len(ad_data)} ads in parallel...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        processed_count = 0
        for future in as_completed(futures):
            ad_id = futures[future]
            processed_count += 1
            try:
                creative_info = future.result()
                if ad_id in ad_data:
                    ad_data[ad_id]['creative_details'] = creative_info
                else:
                    print(f"Warning: ad_id {ad_id} not found in ad_data during creative fetch completion.")
            except Exception as e:
                print(f"Error processing creative future for ad {ad_id}: {e}")
                # 오류 발생 시 기본값 삽입
                if ad_id in ad_data:
                     ad_data[ad_id]['creative_details'] = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            if processed_count % 50 == 0: # 50개 처리마다 로그 출력
                 print(f"  Processed {processed_count}/{len(ad_data)} creatives...")
    print("Finished fetching creative details.")


# --- 메타 광고 데이터 가져오기 및 포맷 함수 ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    """Facebook Insights API에서 광고 데이터를 가져와 포맷합니다."""
    all_records = []
    # ROAS 계산 위해 action_values 추가
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions,action_values'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true', # 권장 기여 설정 사용
        'limit': 200 # 페이지당 데이터 수 증가 시도
    }
    page_count = 1
    while insights_url:
        print(f"Fetching Meta Ads data page {page_count}...")
        current_url = insights_url if page_count > 1 else insights_url
        current_params = params if page_count == 1 else None
        try:
            response = requests.get(url=current_url, params=current_params, timeout=60) # 타임아웃 증가
            response.raise_for_status()
        except requests.exceptions.RequestException as req_err:
            print(f"Meta Ads API network error (Page: {page_count}): {req_err}")
            break # 오류 시 중단하고 현재까지 데이터로 처리
        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page:
            print(f"No more data found on Meta page {page_count}.")
            break
        all_records.extend(records_on_page)
        print(f"Fetched {len(records_on_page)} records from Meta page {page_count}. Total: {len(all_records)}")
        paging = data.get('paging', {})
        insights_url = paging.get('next')
        page_count += 1
        if page_count > 1: params = None # 다음 페이지는 URL만 사용

    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records:
        return {"html_table": "<p>선택한 기간에 대한 Meta 광고 데이터가 없습니다.</p>", "data": []}

    # 데이터 집계 (ad_id 기준)
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id')
        if not ad_id: continue
        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id, 'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'), 'adset_name': record.get('adset_name'),
                'spend': 0.0, 'impressions': 0, 'link_clicks': 0,
                'purchase_count': 0, 'purchase_value': 0.0 # 구매 값 초기화
            }
        # 수치 데이터 누적
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except: pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except: pass
        # 'clicks' 필드는 링크 클릭 수(outbound_clicks 아님)로 가정
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0))
        except: pass

        # 구매 수 (actions) 및 구매 값 (action_values) 집계
        purchase_count_on_record = 0
        purchase_value_on_record = 0.0
        actions = record.get('actions', [])
        if isinstance(actions, list):
            for action in actions:
                # 구매 건수 (표준 'purchase' 액션)
                if action.get("action_type") == "purchase":
                    try: purchase_count_on_record += int(action.get("value", 0))
                    except: pass
        action_values = record.get('action_values', [])
        if isinstance(action_values, list):
            for item in action_values:
                # 구매 값 (표준 웹사이트 구매 관련 액션들)
                 if item.get("action_type") in ["purchase", "offsite_conversion.fb_pixel_purchase", "website_purchase"]:
                    try: purchase_value_on_record += float(item.get("value", 0.0))
                    except: pass

        ad_data[ad_id]['purchase_count'] += purchase_count_on_record
        ad_data[ad_id]['purchase_value'] += purchase_value_on_record

        # 텍스트 정보는 마지막 값으로 덮어쓰기
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # 크리에이티브 정보 가져오기
    fetch_creatives_parallel(ad_data, ver, token)

    result_list = list(ad_data.values())
    if not result_list:
        return {"html_table": "<p>처리할 Meta 광고 데이터가 없습니다.</p>", "data": []}

    df = pd.DataFrame(result_list)

    # 크리에이티브 정보 컬럼 추가
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '-'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details']) # 임시 컬럼 제거

    # 숫자형 변환 및 0 채우기
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count', 'purchase_value']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        if col not in ['purchase_value']: # 구매 값은 소수점 유지 가능성 고려 (여기선 int로 변환)
            df[col] = df[col].round(0).astype(int)
        else:
             df[col] = df[col].round(0).astype(int) # 일단 int로 통일

    # 계산 지표 생성
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)
    # ROAS 계산 (구매전환값 / 광고비)
    df['ROAS'] = df.apply(lambda r: f"{(r['purchase_value'] / r['spend']):.2f}" if r['spend'] > 0 else '0.00', axis=1)

    # 컬럼 이름 변경
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수', 'purchase_value': '구매 전환 값'
    })

    # 합계 행 계산
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_purchase_value = df['구매 전환 값'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0
    total_roas = f"{(total_purchase_value / total_spend):.2f}" if total_spend > 0 else '0.00'

    # 합계 행 Series 생성 (순서 주의)
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '',
        'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks,
        'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases,
        '구매당 비용': total_cpp, '구매 전환 값': total_purchase_value, 'ROAS': total_roas,
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''
    }
    totals_row = pd.Series(totals_data)

    # 컬럼 순서 정의
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '구매 전환 값', 'ROAS',
        'ad_id', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df['광고 성과'] = '' # 컬럼 미리 생성
    # 존재하는 컬럼만 사용하여 순서 맞추기
    df = df[[col for col in column_order if col in df.columns]]

    # 합계 행 추가
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    # 정렬 키 계산 (구매당 비용 기준 오름차순)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1 # 합계 행은 항상 위로
        cost = pd.to_numeric(row.get('구매당 비용', 0), errors='coerce')
        return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)

    # URL 정보 저장 (정렬 전)
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}

    # 정렬 및 불필요 컬럼(정렬키, 임시 URL) 제거
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key', 'display_url', 'target_url'], errors='ignore')

    # 광고 성과 분류 (ad_id 기반)
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    # 유효한 구매당 비용이 있는 행만 필터링
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()

    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
         df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
         # 10만원 미만인 광고 중에서 정렬
         df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
         top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()

    def categorize_performance(row):
        if row['광고명'] == '합계': return ''
        ad_id_current = row.get('ad_id')
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')
        if pd.isna(cost) or cost == 0: return ''
        if cost >= 100000: return '개선 필요!'
        if ad_id_current in top_ad_ids:
            try: rank = top_ad_ids.index(ad_id_current)
            except ValueError: return '' # 리스트에 없는 경우 방지
            if rank == 0: return '위닝 콘텐츠'
            if rank == 1: return '고성과 콘텐츠'
            if rank == 2: return '성과 콘텐츠'
        return ''

    if 'ad_id' in df_sorted.columns:
        df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    else: # ad_id가 없는 경우 처리 (이론상 발생하면 안 됨)
        df_sorted['광고 성과'] = ''

    # HTML 생성을 위해 URL 정보 다시 매핑
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', '')) if 'ad_id' in df_sorted.columns else ''
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', '')) if 'ad_id' in df_sorted.columns else ''


    # --- HTML 테이블 생성 ---
    def format_currency(amount):
        try: return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
        except (ValueError, TypeError): return "0 ₩"
    def format_number(num):
        try: return f"{int(num):,}" if pd.notna(num) else "0"
        except (ValueError, TypeError): return "0"
    def format_roas(roas):
        try: return f"{float(roas):.2f}" if pd.notna(roas) and str(roas) != '' else "0.00"
        except (ValueError, TypeError): return "0.00"

    # HTML 테이블 구조 (ROAS 포함)
    html_table = """
    <style>
      table {border-collapse: collapse; width: 100%; font-family: sans-serif;}
      th, td {padding: 8px; border-bottom: 1px solid #ddd;}
      th {background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle; font-size: 13px;}
      td {text-align: right; white-space: nowrap; vertical-align: middle; font-size: 13px;}
      td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; } /* 광고명, 캠페인명, 광고세트명 왼쪽 정렬 */
      td:nth-child(12), td:nth-child(13), td:nth-child(14) { text-align: center; } /* 광고 성과, 콘텐츠 유형, 광고 콘텐츠 가운데 정렬 */
      tr:hover {background-color: #f5f5f5;}
      .total-row {background-color: #e6f2ff; font-weight: bold;}
      .winning-content {color: #009900; font-weight: bold;}
      .medium-performance {color: #E69900; font-weight: bold;} /* 주황색 계열 */
      .third-performance {color: #FF9900; font-weight: bold;} /* 좀 더 밝은 주황 */
      .needs-improvement {color: #FF0000; font-weight: bold;}
      a {text-decoration: none; color: inherit;}
      img.ad-content-thumbnail {max-width:80px; max-height:80px; vertical-align: middle; border-radius: 4px;}
      td.ad-content-cell { text-align: center; }
    </style>
    <table>
      <thead>
        <tr>
          <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
          <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수</th>
          <th>구매당 비용</th> <th>구매 전환 값</th> <th>ROAS</th>
          <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
        </tr>
      </thead>
      <tbody>
    """
    # 데이터 행 추가
    # iter_df = df_sorted[[col for col in df_sorted.columns if col != 'ad_id']] # HTML 표시용 DataFrame (ad_id 제외)
    iter_df = df_sorted # ad_id가 필요할 수 있으므로 일단 포함

    for index, row in iter_df.iterrows():
        row_class = 'total-row' if row.get('광고명') == '합계' else ''
        performance_text = row.get('광고 성과', '')
        performance_class = ''
        if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
        elif performance_text == '개선 필요!': performance_class = 'needs-improvement'

        display_url = row.get('display_url', '')
        target_url = row.get('target_url', '')
        content_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
            # target_url이 유효한 URL 문자열인지 확인
            if isinstance(target_url, str) and target_url.startswith('http'):
                 content_tag = f'<a href="{target_url}" target="_blank" title="콘텐츠 보기">{img_tag}</a>'
            else: # 유효하지 않은 URL이면 이미지 태그만 표시
                 content_tag = img_tag
        elif row.get('광고명') != '합계': content_tag = "-" # 합계 행 아니면 '-' 표시

        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td> <td>{row.get('캠페인명','')}</td> <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td> <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td> <td>{row.get('CTR','0.00%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td> <td>{format_number(row.get('구매 수',0))}</td>
          <td>{format_currency(row.get('구매당 비용',0))}</td>
          <td>{format_currency(row.get('구매 전환 값',0))}</td>
          <td>{format_roas(row.get('ROAS',0))}</td>
          <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','-')}</td>
          <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """
    html_table += "</tbody></table>" # tbody 추가

    # 최종 JSON 결과 준비 (불필요 컬럼 제외)
    final_columns_for_json = [col for col in df_sorted.columns if col not in ['ad_id', 'display_url', 'target_url']]
    df_for_json = df_sorted[final_columns_for_json]

    # NaN/Inf 및 특수 타입 처리 함수
    def clean_numeric(data):
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
            if math.isinf(data) or math.isnan(data): return 0
            # Numpy 타입을 Python 기본 타입으로 변환
            if hasattr(data, 'item'): return data.item()
            return data
        # Pandas Timestamp 등 처리
        elif isinstance(data, (pd.Timestamp, date)): return data.isoformat()
        elif hasattr(data, 'item'): # Numpy 타입 처리 (위 float/int 외)
             try: return data.item()
             except: return str(data)
        # 기타 처리할 수 없는 타입은 문자열로
        elif not isinstance(data, (str, bool)) and data is not None:
            return str(data)
        return data

    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}


# --- 앱 실행 ---
# (로컬 테스트 시 주석 해제)
# if __name__ == '__main__':
#     # .env 파일 로드
#     # from dotenv import load_dotenv
#     # load_dotenv()
#     # 설정 로드 확인
#     # ACCOUNT_CONFIGS = load_account_configs()
#     # CAFE24_CONFIGS = load_cafe24_configs() # cafe24_api 모듈에서 로드됨
#     # print(f"Loaded Meta accounts: {list(ACCOUNT_CONFIGS.keys())}")
#     # print(f"Loaded Cafe24 configs: {list(CAFE24_CONFIGS.keys())}")
#     # Flask 앱 실행
#     app.run(debug=True, port=5001)
