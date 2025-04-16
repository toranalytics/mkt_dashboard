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
# cafe24_api.py 파일이 같은 폴더에 있어야 함
from .cafe24_api import CAFE24_CONFIGS, process_cafe24_data

app = Flask(__name__)

# --- Meta 계정 설정 로드 ---
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
            print(f"Loaded Meta account: {name} (ID: {account_id})")
            i += 1
        else:
            if i == 1 and not name: pass
            elif name or account_id or token: print(f"Warning: Incomplete Meta account config index {i}.")
            break
    if not accounts: print("Warning: No complete Meta account configurations found.")
    return accounts

ACCOUNT_CONFIGS = load_account_configs()

# CORS 허용
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook & Cafe24 광고 성과 보고서 API"})

@app.route('/api/accounts', methods=['POST'])
def get_accounts():
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
             return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)
    except Exception as e:
        print(f"Error getting account list: {e}")
        traceback.print_exc()
        return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API (최종 수정 버전) ---
@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS': return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        report_password = os.environ.get("REPORT_PASSWORD")
        if report_password and (not password or password != report_password):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date
        print(f"Report requested for date range: {start_date} to {end_date}")

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
        cafe24_totals = {"total_visitors": 0, "total_sales": 0} # 기본값
        selected_cafe24_config = CAFE24_CONFIGS.get(selected_account_key) # Meta 키로 Cafe24 설정 조회

        if selected_cafe24_config:
            print(f"Fetching Cafe24 totals for '{selected_account_key}'...")
            # cafe24_api 모듈의 함수 호출 (기간 총계 반환)
            cafe24_totals = process_cafe24_data(selected_account_key, selected_cafe24_config, start_date, end_date)
            print(f"Cafe24 totals fetch attempted. Visitors: {cafe24_totals.get('total_visitors')}, Sales: {cafe24_totals.get('total_sales')}")
        else:
            print(f"Cafe24 config not found for '{selected_account_key}', skipping Cafe24 totals fetch.")

        # --- 2. Meta 광고 데이터 가져오기 및 최종 보고서 생성 ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        # fetch 함수에 cafe24 총계 데이터 전달
        final_result = fetch_and_format_facebook_ads_data(
            start_date, end_date, meta_api_version, meta_account_id, meta_token,
            cafe24_totals # Cafe24 총계 딕셔너리 전달
        )
        print("Meta Ads data fetch and report generation completed.")

        # --- 3. 결과 반환 ---
        # 최종 결과에 Cafe24 총계 정보를 추가하여 반환 (프론트엔드에서 별도 사용 가능)
        final_result["cafe24_totals"] = cafe24_totals
        print("--- Report generation complete ---")
        return jsonify(final_result)

    except Exception as e:
        error_message = "An internal server error occurred."
        print(f"{error_message} Details: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": error_message}), 500


# --- 메타 광고 크리에이티브 관련 함수들 ---
# (이전과 동일)
def get_creative_details(ad_id, ver, token):
    # ... (이전 코드 복사) ...
    pass
def get_video_source_url(video_id, ver, token):
     # ... (이전 코드 복사) ...
    pass
def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
     # ... (이전 코드 복사) ...
    pass

# --- 메타 광고 데이터 가져오기 및 최종 보고서 생성 함수 (최종 수정본) ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals):
    """
    Facebook Insights API 데이터를 가져와 포맷하고,
    HTML 테이블의 합계 행에 Cafe24 총계 데이터를 추가합니다.
    ROAS와 구매 전환 값은 제외됩니다.
    cafe24_totals: {"total_visitors": int, "total_sales": int} 형태의 딕셔너리
    """
    all_records = []
    # API 요청 필드 (action_values 제외, actions는 구매 수 위해 유지)
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = { 'fields': metrics, 'access_token': token, 'level': 'ad',
               'time_range[since]': start_date, 'time_range[until]': end_date,
               'use_unified_attribution_setting': 'true', 'limit': 200 }
    page_count = 1
    while insights_url:
        print(f"Fetching Meta Ads data page {page_count}...")
        current_url = insights_url if page_count > 1 else insights_url
        current_params = params if page_count == 1 else None
        try:
            response = requests.get(url=current_url, params=current_params, timeout=60)
            response.raise_for_status()
        except requests.exceptions.RequestException as req_err:
            print(f"Meta Ads API network error (Page: {page_count}): {req_err}"); break
        data = response.json(); records_on_page = data.get('data', [])
        if not records_on_page: break
        all_records.extend(records_on_page)
        paging = data.get('paging', {}); insights_url = paging.get('next')
        page_count += 1; params = None

    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records: return {"html_table": "<p>Meta 광고 데이터 없음.</p>", "data": []}

    # 데이터 집계 (ROAS/구매값 제외)
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue
        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id, 'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'), 'adset_name': record.get('adset_name'),
                'spend': 0.0, 'impressions': 0, 'link_clicks': 0, 'purchase_count': 0
            }
        # 수치 누적
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except: pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except: pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0))
        except: pass
        # 구매 수 집계
        purchase_count_on_record = 0
        actions = record.get('actions', [])
        if isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "purchase":
                    try: purchase_count_on_record += int(action.get("value", 0))
                    except: pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record
        # 텍스트 업데이트
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # 크리에이티브 정보 가져오기
    fetch_creatives_parallel(ad_data, ver, token)

    result_list = list(ad_data.values());
    if not result_list: return {"html_table": "<p>Meta 데이터 없음.</p>", "data": []}
    df = pd.DataFrame(result_list)

    # 크리에이티브 컬럼 추가
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '-'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details'])

    # 숫자형 변환
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)

    # 계산 지표 생성
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1) # 이름에서 (Meta) 제거

    # 컬럼 이름 변경
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수' # 이름에서 (Meta) 제거
    })

    # 합계 행 계산
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0

    # --- 합계 행 Series 생성 (Cafe24 총계 값 포함) ---
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '',
        'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks,
        'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases,
        '구매당 비용': total_cpp,
        'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), # 전달받은 총계 사용
        'Cafe24 매출': cafe24_totals.get('total_sales', 0),      # 전달받은 총계 사용
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''
    }
    # --- 최종 컬럼 순서 정의 ---
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용',
        'Cafe24 방문자 수', 'Cafe24 매출', # <-- Cafe24 컬럼 위치
        'ad_id', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    totals_row = pd.Series(totals_data)

    df['광고 성과'] = '' # 컬럼 미리 생성
    # 컬럼 순서 적용 (df에는 아직 Cafe24 컬럼 없음)
    df_meta_columns = [col for col in column_order if col in df.columns and col not in ['Cafe24 방문자 수', 'Cafe24 매출']]
    df = df[df_meta_columns]

    # 합계 행 추가 및 정렬
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용', 0), errors='coerce')
        return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}
    # 정렬 후 sort_key 제거, URL 컬럼은 나중에 추가
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key', 'display_url', 'target_url'], errors='ignore')

    # 광고 성과 분류
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
         df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
         df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
         top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()
    def categorize_performance(row): # 성과 분류 함수
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


    # --- HTML 테이블 생성 (Cafe24 컬럼 포함 및 값 처리) ---
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) else "0"

    # 최종 테이블 컬럼 순서 (HTML 생성용)
    display_columns = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용',
        'Cafe24 방문자 수', 'Cafe24 매출', # <-- Cafe24 컬럼 포함
        '광고 성과', '콘텐츠 유형', '광고 콘텐츠'
    ]

    html_table = """
    <style> /* ... CSS ... */ </style>
    <table>
      <thead>
        <tr>
          <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
          <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수</th>
          <th>구매당 비용</th>
          <th>Cafe24 방문자 수</th><th>Cafe24 매출</th> {/* <-- 헤더 추가 */}
          <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
        </tr>
      </thead>
      <tbody>
    """
    # df_sorted 를 사용하여 행 생성
    for index, row in df_sorted.iterrows():
        row_class = 'total-row' if row.get('광고명') == '합계' else ''
        performance_text = row.get('광고 성과', '')
        performance_class = ''
        if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
        elif performance_text == '개선 필요!': performance_class = 'needs-improvement'

        # 콘텐츠 태그 생성
        display_url = row.get('display_url', ''); target_url = row.get('target_url', '')
        content_tag = ""; img_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
            if isinstance(target_url, str) and target_url.startswith('http'): content_tag = f'<a href="{target_url}" target="_blank" title="콘텐츠 보기">{img_tag}</a>'
            else: content_tag = img_tag
        elif row.get('광고명') != '합계': content_tag = "-"

        # 행 HTML 생성
        html_table += f'<tr class="{row_class}">'
        for col in display_columns: # 정의된 순서대로 셀 생성
             if col == 'FB 광고비용' or col == 'CPC' or col == '구매당 비용' or col == 'Cafe24 매출':
                 value = format_currency(row.get(col)) if row.get('광고명') == '합계' or col != 'Cafe24 매출' else '-' # 합계 행 Cafe24 매출만 포맷, 나머진 '-'
                 if col == 'Cafe24 매출' and row.get('광고명') == '합계': value = format_currency(row.get(col)) # 합계행 매출 포맷
                 elif col == 'Cafe24 매출': value = '-' # 광고행 매출은 '-'

             elif col == '노출' or col == 'Click' or col == '구매 수' or col == 'Cafe24 방문자 수':
                 value = format_number(row.get(col)) if row.get('광고명') == '합계' or col != 'Cafe24 방문자 수' else '-' # 합계 행 Cafe24 방문자수만 포맷, 나머진 '-'
                 if col == 'Cafe24 방문자 수' and row.get('광고명') == '합계': value = format_number(row.get(col)) # 합계행 방문자 포맷
                 elif col == 'Cafe24 방문자 수': value = '-' # 광고행 방문자수는 '-'

             elif col == 'CTR': value = row.get(col, '0.00%')
             elif col == '광고 성과': value = f'<td class="{performance_class}">{performance_text}</td>'; html_table += value; continue # 클래스 포함하여 직접 추가
             elif col == '콘텐츠 유형': value = row.get(col, '-')
             elif col == '광고 콘텐츠': value = f'<td class="ad-content-cell">{content_tag}</td>'; html_table += value; continue # 클래스 포함하여 직접 추가
             else: value = row.get(col, '') # 광고명, 캠페인명 등

             # 정렬 클래스 적용
             td_align = 'left' if col in ['광고명', '캠페인명', '광고세트명'] else ('center' if col in ['광고 성과', '콘텐츠 유형'] else 'right')
             html_table += f'<td style="text-align: {td_align};">{value}</td>'

        html_table += "</tr>\n"
    html_table += "</tbody></table>"

    # JSON 데이터 준비 (ROAS/구매값 제외)
    final_columns_for_json = [col for col in display_columns if col not in ['ad_id', 'display_url', 'target_url', '광고 콘텐츠']] # JSON에는 실제 데이터 컬럼 위주로
    df_for_json = df_sorted[final_columns_for_json].copy() # .copy() 추가

    # clean_numeric 함수 정의 (여기 또는 외부에 정의 필요)
    def clean_numeric(data):
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
            if math.isinf(data) or math.isnan(data): return 0
            if hasattr(data, 'item'): return data.item()
            return data
        elif isinstance(data, (pd.Timestamp, date)): return data.isoformat()
        elif hasattr(data, 'item'):
             try: return data.item()
             except: return str(data)
        elif not isinstance(data, (str, bool)) and data is not None: return str(data)
        return data

    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}


# --- 앱 실행 ---
# if __name__ == '__main__':
#     app.run(debug=True, port=5001)
