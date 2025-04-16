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
from .cafe24_api import CAFE24_CONFIGS, process_cafe24_data

# .env 파일 로드 (필요시)
# from dotenv import load_dotenv
# load_dotenv()

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

# --- 보고서 생성 API (HTML 후처리 방식, ROAS/구매값 제거) ---
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

        # --- 1. Meta 광고 데이터 가져오기 (ROAS/구매값 관련 제거) ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        # fetch 함수는 이제 Cafe24 데이터와 무관
        meta_result = fetch_and_format_facebook_ads_data(start_date, end_date, meta_api_version, meta_account_id, meta_token)
        print("Meta Ads data fetch and processing completed.")

        # --- 2. Cafe24 총계 데이터 가져오기 ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0} # 기본값
        selected_cafe24_config = CAFE24_CONFIGS.get(selected_account_key)

        if selected_cafe24_config:
            print(f"Fetching Cafe24 totals for '{selected_account_key}'...")
            # cafe24_api 모듈의 함수 호출 (기간 총계 반환)
            cafe24_totals = process_cafe24_data(selected_account_key, selected_cafe24_config, start_date, end_date)
            print("Cafe24 totals fetch attempted.")
        else:
            print(f"Cafe24 config not found for '{selected_account_key}', skipping Cafe24 totals fetch.")

        # --- 3. Meta HTML 테이블 후처리 (ROAS/구매값 제거된 기준) ---
        modified_html_table = add_cafe24_totals_to_html(
            meta_result.get("html_table", ""), # 원본 Meta HTML
            cafe24_totals.get("total_visitors", 0),
            cafe24_totals.get("total_sales", 0)
        )

        # --- 4. 최종 결과 조합 ---
        final_result = {
            "meta_report": {
                 "html_table": modified_html_table, # 수정된 HTML
                 "data": meta_result.get("data", []) # 원본 Meta 데이터 (ROAS/구매값 제외됨)
             },
            "cafe24_totals": cafe24_totals # Cafe24 총계는 별도 전달
        }
        print("--- Report generation complete ---")
        return jsonify(final_result)

    except Exception as e:
        error_message = "An internal server error occurred."
        print(f"{error_message} Details: {str(e)}")
        traceback.print_exc()
        return jsonify({"error": error_message}), 500


# --- HTML 테이블 후처리 함수 (ROAS/구매값 제거 기준 수정) ---
def add_cafe24_totals_to_html(original_html, total_visitors, total_sales):
    """Meta HTML 테이블에 Cafe24 총 방문자/매출 컬럼 및 데이터를 추가 (합계 행에만 값 표시)."""
    if not original_html or not isinstance(original_html, str): return original_html

    def format_num(num): return f"{int(num):,}" if pd.notna(num) else "0"
    def format_curr(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"

    try:
        # 1. 헤더(th) 추가: '구매당 비용 (Meta)' 뒤에 추가
        header_find = "<th>구매당 비용 (Meta)</th>"
        header_add = """
          <th>Cafe24 방문자 수</th>
          <th>Cafe24 매출</th>
        """
        # thead 내부의 마지막 th 를 찾아 그 뒤에 삽입 시도 (더 안정적)
        thead_end = original_html.find("</thead>")
        if thead_end == -1: return original_html # thead 없으면 처리 불가
        tr_header_start = original_html.rfind("<tr>", 0, thead_end)
        tr_header_end = original_html.find("</tr>", tr_header_start)
        if tr_header_start == -1 or tr_header_end == -1: return original_html

        last_th_index = original_html.rfind("</th>", tr_header_start, tr_header_end)
        if last_th_index == -1: return original_html

        insert_pos_th = last_th_index + len("</th>")
        modified_html = original_html[:insert_pos_th] + header_add + original_html[insert_pos_th:]


        # 2. 데이터 행(td) 추가
        rows = modified_html.split('<tr')
        new_rows = []
        for i, row_part in enumerate(rows):
            if i == 0: new_rows.append(row_part); continue
            row_html = "<tr" + row_part

            # thead 안의 행은 건너뛰기
            if "<th>" in row_html:
                new_rows.append(row_html)
                continue

            # '구매당 비용 (Meta)' 컬럼의 닫는 td 태그 찾기
            # 컬럼 순서: 광고명(1),캠페인명(2),광고세트명(3),비용(4),노출(5),클릭(6),CTR(7),CPC(8),구매수(9),구매당비용(10)
            # 10번째 </td> 뒤에 삽입
            td_parts = row_html.split("</td>")
            if len(td_parts) > 10: # 컬럼이 충분히 있는지 확인 (헤더 포함 최소 11개 파트)
                insert_pos_td = row_html.find(td_parts[10]) + len(td_parts[10]) + len("</td>") # 10번째 </td> 뒤

                is_total_row = 'class="total-row"' in row_html or '>합계<' in td_parts[0] # 합계 행 확인

                if is_total_row:
                    td_add = f"<td>{format_num(total_visitors)}</td><td>{format_curr(total_sales)}</td>"
                else:
                    td_add = "<td>-</td><td>-</td>" # 빈 칸

                row_html = row_html[:insert_pos_td] + td_add + row_html[insert_pos_td:]
            else:
                 print(f"Warning: Could not find enough <td> tags in row {i} for Cafe24 data insertion.")
                 # 빈칸이라도 추가 시도 (구조 깨짐 방지)
                 last_td_index = row_html.rfind("</td>")
                 if last_td_index != -1:
                      insert_pos = last_td_index + len("</td>")
                      row_html = row_html[:insert_pos] + "<td>-</td><td>-</td>" + row_html[insert_pos:]


            new_rows.append(row_html)

        return "".join(new_rows)

    except Exception as e:
        print(f"Error during HTML post-processing: {e}")
        traceback.print_exc()
        return original_html # 오류 시 원본 반환

# --- 메타 광고 크리에이티브 관련 함수들 ---
# (get_creative_details, get_video_source_url, fetch_creatives_parallel - 이전과 동일)
def get_creative_details(ad_id, ver, token):
    # ... (이전 코드 전체 내용) ...
    pass
def get_video_source_url(video_id, ver, token):
     # ... (이전 코드 전체 내용) ...
    pass
def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
     # ... (이전 코드 전체 내용) ...
    pass

# --- 메타 광고 데이터 가져오기 및 포맷 함수 (ROAS/구매값 제거 버전) ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    """Facebook Insights API 데이터를 가져와 포맷합니다. (ROAS/구매값 제외)"""
    all_records = []
    # API 요청 필드에서 action_values 제거 (구매 값 필요 없음)
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

    # 데이터 집계 (ad_id 기준, 구매 값 관련 제거)
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue
        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id, 'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'), 'adset_name': record.get('adset_name'),
                'spend': 0.0, 'impressions': 0, 'link_clicks': 0,
                'purchase_count': 0 # 구매 수만 집계
            }
        # 수치 데이터 누적
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
        # 텍스트 정보 업데이트
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

    # 숫자형 변환 (구매 값 관련 제거)
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)

    # 계산 지표 생성 (ROAS/구매값 관련 제거)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용 (Meta)'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)

    # 컬럼 이름 변경 (ROAS/구매값 관련 제거)
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수 (Meta)' # 이름 변경
    })

    # 합계 행 계산 (ROAS/구매값 관련 제거)
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum()
    total_meta_purchases = df['구매 수 (Meta)'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_meta_cpp = int(round(total_spend / total_meta_purchases)) if total_meta_purchases > 0 else 0

    # 합계 행 Series (ROAS/구매값 관련 제거)
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '',
        'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks,
        'CTR': total_ctr, 'CPC': total_cpc, '구매 수 (Meta)': total_meta_purchases,
        '구매당 비용 (Meta)': total_meta_cpp,
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''
    }
    totals_row = pd.Series(totals_data)

    # 컬럼 순서 정의 (ROAS/구매값 관련 제거)
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수 (Meta)', '구매당 비용 (Meta)',
        'ad_id', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df['광고 성과'] = ''
    # 컬럼 순서 적용 시점에 totals_row 와 df 의 컬럼이 일치해야 함
    df = df[[col for col in column_order if col in df.columns]] # df에 있는 컬럼만 순서 적용

    # 합계 행 추가 및 정렬
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용 (Meta)', 0), errors='coerce')
        return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key', 'display_url', 'target_url'], errors='ignore')

    # 광고 성과 분류
    # ... (분류 로직은 구매당 비용 (Meta) 기준으로 유지) ...
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용 (Meta)'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
         df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용 (Meta)'])
         df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
         top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()
    def categorize_performance(row): # 성과 분류 함수
        if row['광고명'] == '합계': return ''
        ad_id_current = row.get('ad_id'); cost = pd.to_numeric(row.get('구매당 비용 (Meta)', float('inf')), errors='coerce')
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

    # HTML 테이블 생성 (ROAS/구매값 제외 버전)
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) else "0"

    html_table = """
    <style> /* ... CSS ... */ </style>
    <table>
      <thead>
        <tr>
          <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
          <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수 (Meta)</th>
          <th>구매당 비용 (Meta)</th>
          {/* Cafe24 헤더는 후처리에서 추가됨 */}
          <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
        </tr>
      </thead>
      <tbody>
    """
    iter_df = df_sorted
    for index, row in iter_df.iterrows():
        row_class = 'total-row' if row.get('광고명') == '합계' else ''
        performance_text = row.get('광고 성과', '')
        performance_class = '' # 클래스 지정 로직 필요
        content_tag = "" # 콘텐츠 태그 생성 로직 필요

        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td><td>{row.get('캠페인명','')}</td><td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td><td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td><td>{row.get('CTR','0.00%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td><td>{format_number(row.get('구매 수 (Meta)',0))}</td>
          <td>{format_currency(row.get('구매당 비용 (Meta)',0))}</td>
          <td class="{performance_class}">{performance_text}</td><td>{row.get('콘텐츠 유형','-')}</td>
          <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """
    html_table += "</tbody></table>"

    # JSON 데이터 준비 (ROAS/구매값 제외)
    final_columns_for_json = [col for col in df_sorted.columns if col not in ['ad_id', 'display_url', 'target_url']]
    df_for_json = df_sorted[final_columns_for_json]
    def clean_numeric(data): # NaN/Inf/타입 정리
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
            if math.isinf(data) or math.isnan(data): return 0
            if hasattr(data, 'item'): return data.item() # Handle numpy types
            return data
        elif isinstance(data, (pd.Timestamp, date)): return data.isoformat()
        elif hasattr(data, 'item'): # Handle other numpy types
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
