import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, request

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

        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

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


def get_creative_details(ad_id, ver, token):
    """
    광고 ID를 사용하여 크리에이티브 상세 정보(콘텐츠 유형, 표시 URL, 대상 URL)를 가져옵니다.
    object_type 및 SHARE 유형을 고려하여 가능한 정확하게 분류합니다.
    """
    creative_details = {
        'content_type': '알 수 없음',
        'display_url': '',
        'target_url': ''
    }
    try:
        # 광고 ID로 크리에이티브 ID 가져오기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            # 크리에이티브 상세 정보 가져오기
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

            actual_video_id = video_id or feed_video_id

            # 유형 결정
            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={actual_video_id}"
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
                elif link_data and (link_data.get('image_hash') or link_data.get('image_url')):
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
                    creative_details['target_url'] = f"https://www.instagram.com/p/{story_id.split('_')[1]}/" if story_id and "_" in story_id else thumbnail_url
                else:
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']

    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")

    return creative_details


def get_video_source_url(video_id, ver, token):
    """
    비디오 ID를 사용하여 재생 가능한 비디오 소스 URL을 가져옵니다.
    """
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params)
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except Exception as e:
        print(f"Error fetching video source for video {video_id}: {e}")
        return None


def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    """
    여러 광고의 크리에이티브 정보를 병렬로 가져옵니다.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception:
                creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            ad_data[ad_id]['creative_details'] = creative_info


def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    """
    Facebook API에서 데이터를 가져와 처리한 후 HTML 테이블과 JSON 데이터를 생성합니다.
    클릭 수는 API의 "clicks" 필드를 그대로 사용합니다.
    """
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
        raise Exception(f"성과 데이터 불러오기 오류: {response.status_code} - {response.text}")
    data = response.json()
    records = data.get('data', [])
    ad_data = {}

    # 데이터 집계: API의 "clicks" 값을 사용하여 link_clicks 필드를 채웁니다.
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue

        try:
            record["link_clicks"] = int(record.get("clicks", 0))
        except Exception:
            record["link_clicks"] = 0

        # 구매 수는 actions에서 "purchase" 및 "omni_purchase" 이벤트를 통해 집계합니다.
        purchase_count = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                action_type = action.get("action_type", "")
                try:
                    value = int(action.get("value", 0))
                except (ValueError, TypeError):
                    value = 0
                if action_type == "purchase" or action_type.startswith("omni_purchase"):
                    purchase_count += value
        record["purchase_count"] = purchase_count

        ad_data[ad_id] = record

    # 크리에이티브 정보 병렬 가져오기
    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)

    # DataFrame 변환 및 정리
    result_list = list(ad_data.values())
    df = pd.DataFrame(result_list)
    df['creative_details'] = df['ad_id'].map(lambda ad_id: ad_data.get(ad_id, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details'])
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)
        else:
            df[col] = 0
    df['ctr_val'] = df.apply(lambda r: (r['link_clicks'] / r['impressions'] * 100) if r['impressions'] > 0 else 0, axis=1)
    df['CTR'] = df['ctr_val'].apply(lambda x: f"{round(x, 2)}%")
    df['cpc_val'] = df.apply(lambda r: (r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0, axis=1)
    df['CPC'] = df['cpc_val'].apply(lambda x: round(x) if pd.notna(x) else 0).astype(int)
    df['cost_per_purchase_val'] = df.apply(lambda r: (r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0, axis=1)
    df['구매당 비용'] = df['cost_per_purchase_val'].apply(lambda x: round(x) if pd.notna(x) else 0).astype(int)
    df = df.drop(columns=['ctr_val', 'cpc_val', 'cost_per_purchase_val', 'actions', 'clicks'])
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수'
    })

    # 합계 행 처리
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr_val = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ctr = f"{round(total_ctr_val, 2)}%"
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0
    totals_row = pd.Series([
        '합계', '', '', total_spend, total_impressions, total_clicks,
        total_ctr, total_cpc, total_purchases, total_cpp,
        '', '', '', ''
    ], index=[
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형',
        'display_url', 'target_url'
    ])
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df['광고 성과'] = ''
    df = df[column_order]
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

    # 테이블 정렬
    def custom_sort_key(row):
        if row['광고명'] == '합계':
            return -1
        cost = row.get('구매당 비용', 0)
        return float('inf') if cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns='sort_key')

    # 광고 성과 컬럼 재생성
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid_cost = df_non_total[df_non_total['구매당 비용'] > 0].copy()
    df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용'] < 100000].sort_values(by='구매당 비용', ascending=True)
    top_indices = df_rank_candidates.head(3).index.tolist()
    def categorize_performance(row):
        if row['광고명'] == '합계':
            return ''
        cost = row['구매당 비용']
        if cost == 0:
            return ''
        if cost >= 100000:
            return '개선 필요!'
        if row.name in top_indices:
            rank = top_indices.index(row.name)
            if rank == 0:
                return '위닝 콘텐츠'
            if rank == 1:
                return '고성과 콘텐츠'
            if rank == 2:
                return '성과 콘텐츠'
        return ''
    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)

    # HTML 테이블 생성
    def format_currency(amount):
        return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num):
        return f"{int(num):,}" if pd.notna(num) else "0"
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
    .winning-content {color: #009900; font-weight: bold;}
    .medium-performance {color: #E69900; font-weight: bold;}
    .third-performance {color: #FF9900; font-weight: bold;}
    .needs-improvement {color: #FF0000; font-weight: bold;}
    a {text-decoration: none; color: inherit;}
    img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle;}
    td.ad-content-cell { text-align: center; }
    </style>
    <table>
      <tr>
        <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
        <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수</th>
        <th>구매당 비용</th> <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
      </tr>
    """
    for index, row in df_sorted.iterrows():
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        if performance_text == '위닝 콘텐츠':
            performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠':
            performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠':
            performance_class = 'third-performance'
        elif performance_text == '개선 필요!':
            performance_class = 'needs-improvement'
        else:
            performance_class = ''
        display_url = row.get("display_url", "")
        target_url = row.get("target_url", "")
        content_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="광고 콘텐츠">'
            content_tag = f'<a href="{target_url}" target="_blank">{img_tag}</a>' if target_url else img_tag
        elif row['광고명'] != '합계':
            content_tag = "-"
        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td> <td>{row.get('캠페인명','')}</td> <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td> <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td> <td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td> <td>{format_number(row.get('구매 수',0))}</td>
          <td>{format_currency(row.get('구매당 비용',0))}</td> <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td> <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """
    html_table += "</table>"

    # 최종 결과 준비: JSON 데이터 생성
    df_for_json = df_sorted.drop(columns=['display_url', 'target_url'])
    def clean_numeric(data):
        if isinstance(data, dict):
            return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_numeric(item) for item in data]
        elif isinstance(data, (float, int)):
            if math.isinf(data) or math.isnan(data):
                return 0
        return data
    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}

# Flask 앱 실행 (로컬 테스트 시 주석 해제)
# if __name__ == '__main__':
#     app.run(debug=True, port=5001)
