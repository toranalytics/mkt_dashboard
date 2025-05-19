# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time # 시간 로깅을 위해 추가

import pandas as pd
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# --- 계정 설정 로드 ---
def load_account_configs():
    accounts = {}
    i = 1
    while True:
        name_key = f"ACCOUNT_CONFIG_{i}_NAME"
        id_key = f"ACCOUNT_CONFIG_{i}_ID"
        token_key = f"ACCOUNT_CONFIG_{i}_TOKEN"

        name = os.environ.get(name_key)
        account_id = os.environ.get(id_key)
        token = os.environ.get(token_key)

        if name and account_id and token:
            accounts[name] = {"id": account_id, "token": token, "name": name}
            i += 1
        else:
            break
    if not accounts:
        print("Warning: No account configurations found in environment variables (e.g., ACCOUNT_CONFIG_1_NAME/ID/TOKEN).")
    return accounts

ACCOUNT_CONFIGS = load_account_configs()

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook 광고 성과 보고서 API가 실행 중입니다."})

@app.route('/api/accounts', methods=['POST'])
def get_accounts():
    if request.method == 'OPTIONS': # CORS preflight 요청 처리
        return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)
    except Exception as e:
        print(f"Error getting account list: {e}")
        return jsonify({"error": "Failed to retrieve account list."}), 500

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS': # CORS preflight 요청 처리
        return jsonify({}), 200
    try:
        start_time_total = time.time()
        data = request.get_json()
        if not data:
            return jsonify({"error": "요청 본문이 비어있습니다."}), 400

        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date

        selected_account_key = data.get('selected_account_key')
        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1:
                selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
            else:
                return jsonify({"error": "요청에 'selected_account_key'가 필요합니다. (사용 가능한 계정: " + ", ".join(ACCOUNT_CONFIGS.keys()) + ")"}), 400

        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config:
            return jsonify({"error": f"선택한 계정 키 '{selected_account_key}'에 대한 설정을 찾을 수 없습니다. 사용 가능한 계정: " + ", ".join(ACCOUNT_CONFIGS.keys())}), 404

        account = account_config.get('id')
        token = account_config.get('token')
        if not account or not token:
            print(f"Error: Missing ID or Token for account key '{selected_account_key}' in server configuration.")
            return jsonify({"error": "Server configuration error: Incomplete account credentials."}), 500

        ver = "v19.0" # Facebook Graph API 버전

        # 페이지 파라미터 추가 (프론트엔드에서 전달)
        page = data.get('page', 1) # 기본값 1페이지
        items_per_page = 15 # 페이지당 15개 항목

        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, page, items_per_page)
        
        end_time_total = time.time()
        print(f"[Performance] Total report generation time for page {page}: {end_time_total - start_time_total:.2f} seconds")
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

def get_creative_details(ad_id, ver, token):
    creative_details = {
        'content_type': '알 수 없음',
        'display_url': '',
        'target_url': ''
    }
    try:
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            fields = 'object_type,image_url,thumbnail_url,video_id,effective_object_story_id,object_story_spec{link_data{image_hash,image_url,link,message,name,picture,video_id},video_data{video_id}},instagram_permalink_url,asset_feed_spec{videos{thumbnail_url,video_id},images{hash,url}},effective_instagram_media_id'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params)
            details_response.raise_for_status()
            details_data = details_response.json()

            effective_instagram_media_id = details_data.get('effective_instagram_media_id')
            if effective_instagram_media_id:
                ig_api_url = f"https://graph.facebook.com/v19.0/{effective_instagram_media_id}" # API 버전을 프로젝트 버전에 맞춤
                ig_params = {'fields': 'media_url,media_type,permalink,thumbnail_url', 'access_token': token}
                ig_resp = requests.get(ig_api_url, params=ig_params)
                ig_resp.raise_for_status()
                ig_data = ig_resp.json()
                media_url = ig_data.get('media_url')
                media_type = ig_data.get('media_type')
                # permalink = ig_data.get('permalink') # 사용 안 함
                thumbnail_url_ig = ig_data.get('thumbnail_url', media_url)

                if media_type == 'VIDEO': creative_details['content_type'] = '동영상'
                elif media_type == 'IMAGE': creative_details['content_type'] = '사진'
                else: creative_details['content_type'] = media_type or '알 수 없음'
                creative_details['display_url'] = thumbnail_url_ig or media_url or ""
                creative_details['target_url'] = media_url or "" 
                return creative_details

            object_type = details_data.get('object_type')
            video_id_direct = details_data.get('video_id')
            image_url_direct = details_data.get('image_url')
            thumbnail_url_direct = details_data.get('thumbnail_url')
            instagram_permalink_url = details_data.get('instagram_permalink_url')
            
            story_spec = details_data.get('object_story_spec', {})
            asset_feed_spec = details_data.get('asset_feed_spec', {})

            feed_video_id = None
            feed_thumbnail_url = None
            videos_from_feed = asset_feed_spec.get('videos', [])
            if videos_from_feed:
                feed_video_id = videos_from_feed[0].get('video_id')
                feed_thumbnail_url = videos_from_feed[0].get('thumbnail_url')

            oss_link_data = story_spec.get('link_data', {})
            oss_image_url = oss_link_data.get('image_url') or oss_link_data.get('picture')
            oss_link = oss_link_data.get('link')
            oss_video_id = oss_link_data.get('video_id')
            
            if not oss_video_id and 'video_data' in story_spec:
                 oss_video_id = story_spec.get('video_data', {}).get('video_id')

            actual_video_id = video_id_direct or feed_video_id or oss_video_id

            display_candidate_urls = [thumbnail_url_direct, feed_thumbnail_url, image_url_direct, oss_image_url]
            final_display_url = next((url for url in display_candidate_urls if url), "")


            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = final_display_url
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    creative_details['target_url'] = video_source_url or (f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else final_display_url)
                else:
                    creative_details['target_url'] = final_display_url
            elif object_type == 'PHOTO' or image_url_direct or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = final_display_url
                creative_details['target_url'] = final_display_url
            elif object_type == 'SHARE':
                if feed_video_id: # asset_feed_spec.videos
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = final_display_url
                    video_source_url = get_video_source_url(feed_video_id, ver, token)
                    creative_details['target_url'] = video_source_url or f"https://www.facebook.com/watch/?v={feed_video_id}"
                elif oss_video_id: # object_story_spec.link_data or video_data
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = final_display_url
                    video_source_url = get_video_source_url(oss_video_id, ver, token)
                    creative_details['target_url'] = video_source_url or f"https://www.facebook.com/watch/?v={oss_video_id}"
                elif oss_link_data and (oss_link_data.get('image_hash') or oss_image_url):
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = final_display_url
                    creative_details['target_url'] = oss_link or final_display_url
                elif instagram_permalink_url:
                    creative_details['content_type'] = '사진' # 인스타는 썸네일이 이미지일 가능성 높음
                    if thumbnail_url_direct and 'video' in instagram_permalink_url: # 추론
                         creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = final_display_url
                    creative_details['target_url'] = instagram_permalink_url
                elif thumbnail_url_direct:
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = final_display_url
                    story_id = details_data.get('effective_object_story_id')
                    if story_id and "_" in story_id:
                        creative_details['target_url'] = f"https://www.facebook.com/{story_id.replace('_', '/posts/')}"
                    else:
                        creative_details['target_url'] = final_display_url
                else:
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = final_display_url
                    creative_details['target_url'] = oss_link or final_display_url
            elif thumbnail_url_direct: # Fallback for unknown object_type with thumbnail
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = final_display_url
                creative_details['target_url'] = final_display_url

    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e} (URL: {creative_req_url if 'creative_req_url' in locals() else 'N/A'})")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")
    return creative_details

def get_video_source_url(video_id, ver, token):
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params)
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except Exception as e:
        print(f"Notice: Could not fetch video source for video {video_id}. Error: {e}")
        return None

def fetch_creatives_parallel(ad_ids_with_spend, ver, token, max_workers=5):
    creatives_data = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_ids_with_spend}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
                creatives_data[ad_id] = creative_info
            except Exception as e:
                print(f"Error processing creative future for ad {ad_id}: {e}")
                creatives_data[ad_id] = {'content_type': '오류', 'display_url': '', 'target_url': ''}
    return creatives_data

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, page_number, items_per_page):
    s_time_func = time.time()
    all_records = []
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true',
        'limit': 30 # API 호출당 레코드 수 (테스트를 통해 조절)
    }

    s_time_insights = time.time()
    api_page_count = 1
    temp_insights_url = insights_url # next url을 위해 임시 변수 사용
    
    while temp_insights_url:
        s_time_page_fetch = time.time()
        current_url_req = temp_insights_url
        current_params_req = params if api_page_count == 1 else None # 첫 페이지만 파라미터 사용

        try:
            response = requests.get(url=current_url_req, params=current_params_req)
            response.raise_for_status()
        except requests.exceptions.RequestException as req_err:
            print(f"API 페이지 데이터 불러오기 중 오류 (Page: {api_page_count}, URL: {current_url_req.split('access_token=')[0]}...): {req_err}")
            break 
        
        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page:
            print(f"API 페이지 {api_page_count}에서 더 이상 데이터가 없습니다.")
            break
        
        all_records.extend(records_on_page)
        e_time_page_fetch = time.time()
        print(f"[Performance] Fetched {len(records_on_page)} records from API page {api_page_count} in {e_time_page_fetch - s_time_page_fetch:.2f}s. Total records: {len(all_records)}")
        
        paging = data.get('paging', {})
        temp_insights_url = paging.get('next') # 다음 페이지 URL 업데이트
        api_page_count += 1

    e_time_insights = time.time()
    print(f"[Performance] Finished fetching all insights ({len(all_records)} records) in {e_time_insights - s_time_insights:.2f} seconds.")

    if not all_records:
        return {"html_table": "<p>선택한 기간 및 계정에 대한 데이터가 없습니다.</p>", "data": [], "pagination": None}

    s_time_process_records = time.time()
    ad_data = {}
    ad_ids_with_spend = set()
    for record in all_records:
        ad_id = record.get('ad_id')
        if not ad_id: continue
        spend = float(record.get('spend', 0) or 0)
        # if spend == 0: continue # 광고비 0인 광고도 일단 포함 (크리에이티브 요청 대상에서만 제외 고려)
        
        if spend > 0: # 지출이 있는 경우에만 크리에이티브 요청 대상에 추가
            ad_ids_with_spend.add(ad_id)

        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id, 'ad_name': record.get('ad_name'), 
                'campaign_name': record.get('campaign_name'),
                'adset_name': record.get('adset_name'), 'spend': 0.0, 
                'impressions': 0, 'link_clicks': 0, 'purchase_count': 0,
            }
        ad_data[ad_id]['spend'] += spend
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0))
        except (ValueError, TypeError): pass
        
        purchase_on_record = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "purchase":
                    try: purchase_on_record += int(action.get("value", 0))
                    except (ValueError, TypeError): pass
        ad_data[ad_id]['purchase_count'] += purchase_on_record
        
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']
    
    e_time_process_records = time.time()
    print(f"[Performance] Processing {len(all_records)} records into ad_data dict: {e_time_process_records - s_time_process_records:.2f}s. Ads with spend: {len(ad_ids_with_spend)}")

    if not ad_data:
        return {"html_table": "<p>데이터 집계 후 처리할 레코드가 없습니다.</p>", "data": [], "pagination": None}

    s_time_fetch_creatives = time.time()
    creative_info_map = {}
    if ad_ids_with_spend: # 지출이 있는 광고에 대해서만 크리에이티브 정보 요청
        creative_info_map = fetch_creatives_parallel(list(ad_ids_with_spend), ver, token, max_workers=10)
    e_time_fetch_creatives = time.time()
    print(f"[Performance] Fetching creatives for {len(ad_ids_with_spend)} ads: {e_time_fetch_creatives - s_time_fetch_creatives:.2f}s")

    s_time_df_creation = time.time()
    df = pd.DataFrame(list(ad_data.values()))
    if df.empty:
         return {"html_table": "<p>DataFrame이 비어있습니다.</p>", "data": [], "pagination": None}


    df['creative_details'] = df['ad_id'].map(lambda ad_id_map: creative_info_map.get(ad_id_map, {'content_type': 'N/A (지출없음)', 'display_url': '', 'target_url': ''}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details'])

    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df[col] = df[col].astype(int) if col != 'spend' else df[col].round(0).astype(int)

    df['CTR_val'] = 0.0
    df.loc[df['impressions'] > 0, 'CTR_val'] = (df['link_clicks'] / df['impressions'] * 100)
    df['CTR'] = df['CTR_val'].round(2).astype(str) + '%'
    
    df['CPC_val'] = 0.0 # float으로 초기화
    df.loc[df['link_clicks'] > 0, 'CPC_val'] = (df['spend'] / df['link_clicks'])
    df['CPC'] = df['CPC_val'].round(0).astype(int)

    df['CVR_val'] = 0.0
    df.loc[df['link_clicks'] > 0, 'CVR_val'] = (df['purchase_count'] / df['link_clicks'] * 100)
    df['CVR'] = df['CVR_val'].round(2).astype(str) + '%'
    
    df['구매당 비용_val'] = 0.0 # float으로 초기화
    df.loc[df['purchase_count'] > 0, '구매당 비용_val'] = (df['spend'] / df['purchase_count'])
    df['구매당 비용'] = df['구매당 비용_val'].round(0).astype(int)
    
    df = df.drop(columns=['CTR_val', 'CPC_val', 'CVR_val', '구매당 비용_val'])

    df = df.rename(columns={
        'ad_name': '소재명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수'
    })

    column_order = [
        '캠페인명', '광고세트명', '소재명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', 'CVR',
        '구매 수', '구매당 비용', 'ad_id', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    for col in column_order:
        if col not in df.columns: df[col] = ""
    df = df[column_order]
    e_time_df_creation = time.time()
    print(f"[Performance] DataFrame creation and initial processing: {e_time_df_creation - s_time_df_creation:.2f}s")

    s_time_df_aggregation_sort = time.time()
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr_val = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ctr = f"{round(total_ctr_val, 2)}%"
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cvr = f"{round((total_purchases / total_clicks * 100), 2)}%" if total_clicks > 0 else "0%"
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0
    
    totals_row_data = {
        '캠페인명': '', '광고세트명': '', '소재명': '합계', 'FB 광고비용': total_spend,
        '노출': total_impressions, 'Click': total_clicks, 'CTR': total_ctr,
        'CPC': total_cpc, 'CVR': total_cvr, '구매 수': total_purchases,
        '구매당 비용': total_cpp, 'ad_id': '--TOTAL--', '광고 성과': '', '콘텐츠 유형': '', # ad_id를 고유하게 표시
        'display_url': '', 'target_url': ''
    }
    totals_row_df = pd.DataFrame([totals_row_data]) # DataFrame으로 생성
    
    # 합계 행을 제외한 데이터로 정렬 및 성과 계산
    df_data_only = df.copy()
    
    def custom_sort_key_data(row):
        cost = row.get('구매당 비용', float('inf'))
        try:
            cost_num = float(cost)
            return float('inf') if math.isnan(cost_num) or math.isinf(cost_num) or cost_num == 0 else cost_num
        except (ValueError, TypeError): return float('inf')

    df_data_only['sort_key'] = df_data_only.apply(custom_sort_key_data, axis=1)
    df_sorted_data_only = df_data_only.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'])
    
    # 성과 분류
    df_valid_cost = df_sorted_data_only[pd.to_numeric(df_sorted_data_only['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_indices = []
    if not df_valid_cost.empty:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_indices = df_rank_candidates.head(3).index.tolist() # df_sorted_data_only의 인덱스

    def categorize_performance(row):
        cost = float(row['구매당 비용'])
        if math.isnan(cost) or math.isinf(cost) or cost == 0: return ''
        if cost >= 100000: return '개선 필요!'
        if row.name in top_indices: # row.name은 df_sorted_data_only의 인덱스
            rank = top_indices.index(row.name)
            if rank == 0: return '위닝 콘텐츠'
            if rank == 1: return '고성과 콘텐츠'
            if rank == 2: return '성과 콘텐츠'
        return ''
        
    df_sorted_data_only['광고 성과'] = '' # 초기화
    if not df_sorted_data_only.empty: # 데이터가 있을 때만 apply
        df_sorted_data_only['광고 성과'] = df_sorted_data_only.apply(categorize_performance, axis=1)

    # 합계 행과 정렬된 데이터 합치기
    df_sorted_all_data = pd.concat([totals_row_df, df_sorted_data_only], ignore_index=True)
    
    e_time_df_aggregation_sort = time.time()
    print(f"[Performance] DataFrame aggregation, sorting, and performance categorization: {e_time_df_aggregation_sort - s_time_df_aggregation_sort:.2f}s")

    s_time_pagination_slice = time.time()
    total_items = len(df_sorted_all_data)
    total_pages = math.ceil(total_items / items_per_page) if items_per_page > 0 else 1
    
    start_index = (page_number - 1) * items_per_page
    end_index = start_index + items_per_page
    df_paginated = df_sorted_all_data.iloc[start_index:end_index]
    e_time_pagination_slice = time.time()
    print(f"[Performance] Pagination slicing: {e_time_pagination_slice - s_time_pagination_slice:.2f}s")

    s_time_html_render = time.time()
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) and isinstance(amount, (int, float)) and not (math.isnan(amount) or math.isinf(amount)) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) and isinstance(num, (int, float)) and not (math.isnan(num) or math.isinf(num)) else "0"

    html_table_rows = []
    header_row = "<tr><th>캠페인명</th><th>광고세트명</th><th>소재명</th><th>FB 광고비용</th><th>노출</th><th>Click</th><th>CTR</th><th>CPC</th><th>CVR</th><th>구매 수</th><th>구매당 비용</th><th>광고 성과</th><th>콘텐츠 유형</th><th>광고 콘텐츠</th></tr>"
    html_table_rows.append(header_row)

    for index, row in df_paginated.iterrows():
        row_class = 'total-row' if row['소재명'] == '합계' else ''
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
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="광고 콘텐츠">'
            content_tag = f'<a href="{target_url}" target="_blank">{img_tag}</a>' if isinstance(target_url, str) and target_url.startswith('http') else img_tag
        elif row['소재명'] != '합계': content_tag = "-"

        html_table_rows.append(f"""
        <tr class="{row_class}">
          <td>{row.get('캠페인명','')}</td><td>{row.get('광고세트명','')}</td><td>{row.get('소재명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td><td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td><td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td><td>{row.get('CVR','0%')}</td>
          <td>{format_number(row.get('구매 수',0))}</td><td>{format_currency(row.get('구매당 비용',0))}</td>
          <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td><td class="ad-content-cell">{content_tag}</td>
        </tr>""")

    html_table_style = """
    <style>
    table {border-collapse: collapse; width: 100%;}
    th, td {padding: 8px; border-bottom: 1px solid #ddd;}
    th {background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle;}
    td {text-align: right; white-space: nowrap; vertical-align: middle;}
    td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; }
    td:nth-child(12), td:nth-child(13) { text-align: center; }
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;}
    .medium-performance {color: #E69900; font-weight: bold;}
    .third-performance {color: #FF9900; font-weight: bold;}
    .needs-improvement {color: #FF0000; font-weight: bold;}
    a {text-decoration: none; color: inherit;}
    img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle; border-radius: 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.07);}
    td.ad-content-cell { text-align: center; }
    </style>
    """
    html_table_full = f"{html_table_style}<table>{''.join(html_table_rows)}</table>"
    e_time_html_render = time.time()
    print(f"[Performance] HTML table rendering for page: {e_time_html_render - s_time_html_render:.2f}s")

    # display_url, target_url은 JSON 응답에서 제외
    df_for_json_paginated = df_paginated.drop(columns=['ad_id', 'display_url', 'target_url'], errors='ignore')
    
    def clean_numeric(data):
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
            if math.isinf(data) or math.isnan(data): return None # JSON에서 null로 표현
            return data
        elif pd.isna(data): return None # Pandas NA 처리
        elif not isinstance(data, (str, bool)) and data is not None:
            try: 
                if hasattr(data, 'item'): return data.item()
            except: pass
            return str(data)
        return data

    records_paginated = df_for_json_paginated.to_dict(orient='records')
    cleaned_records_paginated = clean_numeric(records_paginated)
    
    e_time_func = time.time()
    print(f"[Performance] fetch_and_format_facebook_ads_data function total time for page {page_number}: {e_time_func - s_time_func:.2f}s.")

    return {
        "html_table": html_table_full,
        "data": cleaned_records_paginated,
        "pagination": {
            "current_page": page_number,
            "items_per_page": items_per_page,
            "total_items": total_items,
            "total_pages": total_pages
        }
    }

# 로컬 테스트용 (Vercel 배포 시 주석 처리 또는 삭제)
# if __name__ == '__main__':
#     os.environ["REPORT_PASSWORD"] = "your_password" # 테스트용 비밀번호
#     os.environ["ACCOUNT_CONFIG_1_NAME"] = "TestAccount"
#     os.environ["ACCOUNT_CONFIG_1_ID"] = "act_your_facebook_ad_account_id" # 실제 광고 계정 ID
#     os.environ["ACCOUNT_CONFIG_1_TOKEN"] = "your_facebook_access_token" # 실제 액세스 토큰
#     ACCOUNT_CONFIGS = load_account_configs()
#     app.run(debug=True, port=5001)
