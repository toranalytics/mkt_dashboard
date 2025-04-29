# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

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
    if request.method == 'OPTIONS':
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
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        data = request.get_json()
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

        ver = "v19.0"
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
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
        creative_params = {'fields': 'creative', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            fields = 'object_type,image_url,thumbnail_url,video_id,effective_object_story_id,object_story_spec,instagram_permalink_url,asset_feed_spec'
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

            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else (f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url'])
                else:
                    creative_details['target_url'] = creative_details['display_url']

            elif object_type == 'PHOTO' or image_url or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = creative_details['display_url']

            elif object_type == 'SHARE':
                if videos_from_feed:
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = feed_thumbnail_url or thumbnail_url or ""
                    if feed_video_id:
                        video_source_url = get_video_source_url(feed_video_id, ver, token)
                        creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={feed_video_id}"
                    else:
                        creative_details['target_url'] = creative_details['display_url']
                elif link_data and oss_video_id:
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or oss_image_url or ""
                    video_source_url = get_video_source_url(oss_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={oss_video_id}"
                elif link_data and (link_data.get('image_hash') or oss_image_url):
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']
                elif instagram_permalink_url:
                    creative_details['content_type'] = '동영상' if thumbnail_url else '사진'
                    creative_details['display_url'] = thumbnail_url or image_url or ""
                    creative_details['target_url'] = instagram_permalink_url
                elif thumbnail_url:
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = thumbnail_url
                    story_id = details_data.get('effective_object_story_id')
                    if story_id and "_" in story_id:
                        creative_details['target_url'] = f"https://www.facebook.com/{story_id}"
                    else:
                        creative_details['target_url'] = thumbnail_url
                else:
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']
            elif thumbnail_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = thumbnail_url
                creative_details['target_url'] = creative_details['display_url']

    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
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
        print(f"Notice: Could not fetch video source for video {video_id}. Might lack permissions or video is private. Error: {e}")
        return None

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception as e:
                print(f"Error processing creative future for ad {ad_id}: {e}")
                creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            if ad_id in ad_data:
                ad_data[ad_id]['creative_details'] = creative_info
            else:
                print(f"Warning: ad_id {ad_id} not found in ad_data during creative fetch completion.")

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
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
        'limit': 100
    }

    page_count = 1
    while insights_url:
        current_url = insights_url if page_count > 1 else insights_url
        current_params = params if page_count == 1 else None
        try:
            response = requests.get(url=current_url, params=current_params)
            response.raise_for_status()
        except requests.exceptions.RequestException as req_err:
            print(f"페이지 데이터 불러오기 중 네트워크 오류 발생 (Page: {page_count}, URL: {current_url.split('access_token=')[0]}...): {req_err}")
            print(f"현재까지 수집된 데이터로 보고서를 생성합니다.")
            break

        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page:
            if page_count == 1: print("첫 페이지에서 데이터를 찾을 수 없습니다.")
            else: print(f"페이지 {page_count}에서 더 이상 데이터가 없습니다. 중단합니다.")
            break

        all_records.extend(records_on_page)
        print(f"Fetched {len(records_on_page)} records from page {page_count}. Total records: {len(all_records)}")

        paging = data.get('paging', {})
        insights_url = paging.get('next')
        page_count += 1
        if page_count > 1:
            params = None

    print(f"Finished fetching all pages. Total {len(all_records)} records found.")

    if not all_records:
        print("처리할 데이터가 없습니다.")
        return {"html_table": "<p>선택한 기간 및 계정에 대한 데이터가 없습니다.</p>", "data": []}

    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue
        spend = float(record.get('spend', 0) or 0)
        if spend == 0:
            continue
        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id,
                'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'),
                'adset_name': record.get('adset_name'),
                'spend': 0.0,
                'impressions': 0,
                'link_clicks': 0,
                'purchase_count': 0,
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

    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)

    result_list = list(ad_data.values())
    if not result_list:
        print("데이터 집계 후 처리할 레코드가 없습니다.")
        return {"html_table": "<p>데이터가 없습니다.</p>", "data": []}

    df = pd.DataFrame(result_list)
    df['creative_details'] = df['ad_id'].map(lambda ad_id: ad_data.get(ad_id, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details'])

    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'spend':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        else:
            df[col] = 0

    df['CTR'] = df.apply(lambda r: f"{round((r['link_clicks'] / r['impressions'] * 100), 2)}%" if r['impressions'] > 0 else "0%", axis=1)
    df['CPC'] = df.apply(lambda r: round(r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0, axis=1).astype(int)
    df['CVR'] = df.apply(lambda r: f"{round((r['purchase_count'] / r['link_clicks'] * 100), 2)}%" if r['link_clicks'] > 0 else "0%", axis=1)
    df['구매당 비용'] = df.apply(lambda r: round(r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0, axis=1).astype(int)

    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수'
    })

    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', 'CVR', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df = df[[col for col in column_order if col in df.columns]]

    # 합계 행
    total_row = {
        '광고명': '합계',
        '캠페인명': '',
        '광고세트명': '',
        'FB 광고비용': df['FB 광고비용'].sum(),
        '노출': df['노출'].sum(),
        'Click': df['Click'].sum(),
        'CTR': '', 'CPC': '', 'CVR': '',
        '구매 수': df['구매 수'].sum(),
        '구매당 비용': '',
        '광고 성과': '',
        '콘텐츠 유형': '',
        'display_url': '',
        'target_url': ''
    }

    # 평균 행
    avg_ctr = f"{round(df['Click'].sum()/df['노출'].sum()*100,2)}%" if df['노출'].sum() > 0 else "0%"
    avg_cpc = f"{int(df['FB 광고비용'].sum()/df['Click'].sum())} ₩" if df['Click'].sum() > 0 else "0 ₩"
    avg_cpp = f"{int(df['FB 광고비용'].sum()/df['구매 수'].sum())} ₩" if df['구매 수'].sum() > 0 else "0 ₩"
    avg_row = {
        '광고명': '평균',
        '캠페인명': '', '광고세트명': '', 'FB 광고비용': '', '노출': '', 'Click': '',
        'CTR': avg_ctr, 'CPC': avg_cpc, 'CVR': '',
        '구매 수': '', '구매당 비용': avg_cpp,
        '광고 성과': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': ''
    }

    df_with_total = pd.concat([pd.DataFrame([total_row, avg_row]), df], ignore_index=True)

    # 광고 성과 라벨링
    def categorize_performance(row, top_indices):
        if row['광고명'] == '합계' or row['광고명'] == '평균': return ''
        try:
            cost = float(row['구매당 비용'])
            if math.isnan(cost) or math.isinf(cost) or cost == 0: return ''
            if cost >= 100000: return '개선 필요!'
            if row.name in top_indices:
                rank = top_indices.index(row.name)
                if rank == 0: return '위닝 콘텐츠'
                if rank == 1: return '고성과 콘텐츠'
                if rank == 2: return '성과 콘텐츠'
            return ''
        except (ValueError, TypeError):
            return ''

    df_non_total = df_with_total[(df_with_total['광고명'] != '합계') & (df_with_total['광고명'] != '평균')].copy()
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(0) > 0].copy()
    if not df_valid_cost.empty:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_indices = df_rank_candidates.head(3).index.tolist()
    else:
        top_indices = []

    df_with_total['광고 성과'] = df_with_total.apply(lambda row: categorize_performance(row, top_indices), axis=1)

    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) and isinstance(amount, (int, float)) and not (math.isnan(amount) or math.isinf(amount)) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) and isinstance(num, (int, float)) and not (math.isnan(num) or math.isinf(num)) else "0"

    html_table = """
    <style>
    table {border-collapse: collapse; width: 100%;}
    th, td {padding: 8px; border-bottom: 1px solid #ddd;}
    th {background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle;}
    td {text-align: right; white-space: nowrap; vertical-align: middle;}
    td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; }
    td:nth-child(11), td:nth-child(12) { text-align: center; }
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .avg-row {background-color: #f9f9d1; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;}
    .medium-performance {color: #E69900; font-weight: bold;}
    .third-performance {color: #FF9900; font-weight: bold;}
    .needs-improvement {color: #FF0000; font-weight: bold;}
    a {text-decoration: none; color: inherit;}
    img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle;}
    td.ad-content-cell { text-align: center; }
    .sort-icon { font-size: 0.8em; margin-left: 4px; cursor: pointer; }
    </style>
    <table>
      <tr>
        <th>광고명 <span class="sort-icon"></span></th>
        <th>캠페인명 <span class="sort-icon"></span></th>
        <th>광고세트명 <span class="sort-icon"></span></th>
        <th>FB 광고비용 <span class="sort-icon"></span></th>
        <th>노출 <span class="sort-icon"></span></th>
        <th>Click <span class="sort-icon"></span></th>
        <th>CTR <span class="sort-icon"></span></th>
        <th>CPC <span class="sort-icon"></span></th>
        <th>CVR <span class="sort-icon"></span></th>
        <th>구매 수 <span class="sort-icon"></span></th>
        <th>구매당 비용 <span class="sort-icon"></span></th>
        <th>광고 성과 <span class="sort-icon"></span></th>
        <th>콘텐츠 유형 <span class="sort-icon"></span></th>
        <th>광고 콘텐츠 <span class="sort-icon"></span></th>
      </tr>
    """
    # HTML 행 생성
    for idx, row in df_with_total.iterrows():
        row_class = ''
        if row['광고명'] == '합계': row_class = 'total-row'
        if row['광고명'] == '평균': row_class = 'avg-row'
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
        elif row['광고명'] not in ['합계', '평균']: content_tag = "-"

        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td>
          <td>{row.get('캠페인명','')}</td>
          <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td>
          <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td>
          <td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td>
          <td>{row.get('CVR','0%')}</td>
          <td>{format_number(row.get('구매 수',0))}</td>
          <td>{format_currency(row.get('구매당 비용',0))}</td>
          <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td>
          <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """
    html_table += "</table>"

    # JSON 반환
    df_for_json = df_with_total.drop(columns=['display_url', 'target_url'], errors='ignore')
    def clean_numeric(data):
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
            if math.isinf(data) or math.isnan(data): return 0
            return data
        elif not isinstance(data, (str, bool)) and data is not None:
            try: 
                if hasattr(data, 'item'): return data.item()
            except: pass
            return str(data)
        return data

    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}

# Flask 앱 실행 (로컬 테스트 시 주석 해제)
# if __name__ == '__main__':
#     app.run(debug=True, port=5001)
