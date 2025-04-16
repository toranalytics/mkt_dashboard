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

# .env 파일 로드 (Vercel 등에서는 환경 변수로 직접 설정)
# from dotenv import load_dotenv
# load_dotenv()

app = Flask(__name__)

# --- Meta 계정 설정 로드 ---
def load_account_configs():
    # ... (이전과 동일) ...
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
     # ... (이전과 동일, Meta 계정 목록 반환) ...
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

# --- 보고서 생성 API (HTML 후처리 방식) ---
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
            else: return jsonify({"error": "설정된 Meta 계정 없음."}), 400

        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config: return jsonify({"error": f"선택한 Meta 계정 키 '{selected_account_key}' 설정 없음."}), 404

        meta_account_id = account_config.get('id')
        meta_token = account_config.get('token')
        if not meta_account_id or not meta_token: return jsonify({"error": f"Meta 계정 '{selected_account_key}' 설정 오류."}), 500

        meta_api_version = "v19.0"

        # --- 1. Meta 광고 데이터 가져오기 (Cafe24 정보 없이) ---
        print(f"Fetching Meta Ads data for '{selected_account_key}' (ID: {meta_account_id})...")
        meta_result = fetch_and_format_facebook_ads_data(start_date, end_date, meta_api_version, meta_account_id, meta_token)
        print("Meta Ads data fetch and processing completed.")

        # --- 2. Cafe24 총계 데이터 가져오기 ---
        cafe24_totals = {"total_visitors": 0, "total_sales": 0} # 기본값
        selected_cafe24_config = CAFE24_CONFIGS.get(selected_account_key)

        if selected_cafe24_config:
            print(f"Fetching Cafe24 totals for '{selected_account_key}'...")
            cafe24_totals = process_cafe24_data(selected_account_key, selected_cafe24_config, start_date, end_date)
            print("Cafe24 totals fetch attempted.")
        else:
            print(f"Cafe24 config not found for '{selected_account_key}', skipping Cafe24 totals fetch.")

        # --- 3. Meta HTML 테이블 후처리하여 Cafe24 총계 삽입 ---
        modified_html_table = add_cafe24_totals_to_html(
            meta_result.get("html_table", ""), # 원본 HTML
            cafe24_totals.get("total_visitors", 0),
            cafe24_totals.get("total_sales", 0)
        )

        # --- 4. 최종 결과 조합 ---
        final_result = {
            "meta_report": { # 기존 meta_report 구조 유지
                 "html_table": modified_html_table, # 수정된 HTML 테이블
                 "data": meta_result.get("data", []) # 원본 메타 데이터
             },
            "cafe24_totals": cafe24_totals # 카페24 총계 정보 별도 제공
        }
        print("--- Report generation complete (with HTML post-processing) ---")
        return jsonify(final_result)

    # --- 오류 처리 ---
    except Exception as e: # 포괄적인 오류 처리
        error_message = "An internal server error occurred."
        print(f"{error_message} Details: {str(e)}")
        traceback.print_exc() # 서버 로그에 상세 오류 출력
        return jsonify({"error": error_message}), 500


# --- HTML 테이블 후처리 함수 ---
def add_cafe24_totals_to_html(original_html, total_visitors, total_sales):
    """
    생성된 Meta 광고 HTML 테이블 문자열에 Cafe24 총 방문자/매출 컬럼 및 데이터를 추가합니다.
    합계 행에만 값을 넣고 나머지 행은 빈 칸으로 채웁니다.
    """
    if not original_html or not isinstance(original_html, str):
        return original_html # 원본 HTML이 없으면 그대로 반환

    # 숫자 포맷 함수 (HTML 삽입용)
    def format_num(num): return f"{int(num):,}" if pd.notna(num) else "0"
    def format_curr(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"

    try:
        # 1. 헤더(th) 추가
        header_find = "<th>ROAS (Meta)</th>" # 마지막 메타 컬럼 헤더
        header_add = """
          <th>Cafe24 방문자 수</th>
          <th>Cafe24 매출</th>
        """
        modified_html = original_html.replace(header_find, header_find + header_add, 1) # 1번만 교체

        # 2. 데이터 행(td) 추가 (합계 행과 나머지 행 분리 처리)
        rows = modified_html.split('<tr') # 행 기준으로 나누기
        new_rows = []
        for i, row_part in enumerate(rows):
            if i == 0: # 첫 부분 (테이블 시작 태그 등)
                new_rows.append(row_part)
                continue

            row_html = "<tr" + row_part # 완전한 행 HTML 복원

            # 마지막 메타 데이터 컬럼(ROAS (Meta))의 닫는 태그 찾기
            td_find = "</td>" # 가장 마지막 </td>를 기준으로 삽입 시도 (ROAS (Meta) 다음)
            td_add_blank = "<td>-</td><td>-</td>" # 빈 칸
            td_add_total = f"<td>{format_num(total_visitors)}</td><td>{format_curr(total_sales)}</td>" # 합계 값

            # 합계 행인지 확인 (더 견고한 방법: class="total-row" 확인)
            is_total_row = 'class="total-row"' in row_html or '>합계</td>' in row_html

            # 마지막 </td> 찾아서 그 뒤에 추가
            last_td_index = row_html.rfind(td_find)
            if last_td_index != -1:
                 insert_pos = last_td_index + len(td_find)
                 if is_total_row:
                     row_html = row_html[:insert_pos] + td_add_total + row_html[insert_pos:]
                 else:
                     # thead 안의 th 행은 건너뛰기
                     if "<th>" not in row_html:
                          row_html = row_html[:insert_pos] + td_add_blank + row_html[insert_pos:]
            else:
                 # 예상치 못한 구조면 원본 행 유지
                 print(f"Warning: Could not find closing </td> tag in row {i}. Skipping Cafe24 data insertion.")
                 pass

            new_rows.append(row_html)

        return "".join(new_rows) # 수정된 행들을 다시 합쳐서 반환

    except Exception as e:
        print(f"Error during HTML post-processing: {e}")
        traceback.print_exc()
        return original_html # 오류 시 원본 HTML 반환


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

# --- 메타 광고 데이터 가져오기 및 포맷 함수 ---
# (Cafe24 관련 로직 제거됨 - ROAS 등 메타 데이터 처리만 수행)
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    """Facebook Insights API 데이터를 가져와 포맷합니다. (Cafe24 데이터 제외)"""
    all_records = []
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions,action_values'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = { 'fields': metrics, 'access_token': token, 'level': 'ad',
               'time_range[since]': start_date, 'time_range[until]': end_date,
               'use_unified_attribution_setting': 'true', 'limit': 200 }
    page_count = 1
    while insights_url:
        # ... (페이지네이션 로직 - 이전과 동일) ...
        pass # 페이지네이션 구현 필요
        # 임시: 첫 페이지만 가져오도록 단순화
        print(f"Fetching Meta Ads data page {page_count}...")
        try:
            response = requests.get(url=insights_url, params=params, timeout=60) if page_count == 1 else requests.get(url=insights_url, timeout=60)
            response.raise_for_status()
            data = response.json()
            records_on_page = data.get('data', [])
            all_records.extend(records_on_page)
            paging = data.get('paging', {})
            insights_url = paging.get('next') # 다음 페이지 URL
            page_count += 1
            params = None # 다음 요청부터는 URL만 사용
            if not insights_url or not records_on_page : # 다음 페이지 없거나 데이터 없으면 중단
                break
        except requests.exceptions.RequestException as req_err:
            print(f"Meta Ads API network error (Page: {page_count}): {req_err}")
            break

    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records: return {"html_table": "<p>Meta 광고 데이터 없음.</p>", "data": []}

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
                'purchase_count': 0, 'purchase_value': 0.0
            }
        # ... (수치 데이터 및 구매 수/값 집계 로직 - 이전과 동일) ...
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except: pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except: pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0))
        except: pass
        purchase_count_on_record = 0; purchase_value_on_record = 0.0
        actions = record.get('actions', []); action_values = record.get('action_values', [])
        if isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "purchase":
                    try: purchase_count_on_record += int(action.get("value", 0))
                    except: pass
        if isinstance(action_values, list):
            for item in action_values:
                 if item.get("action_type") in ["purchase", "offsite_conversion.fb_pixel_purchase", "website_purchase"]:
                    try: purchase_value_on_record += float(item.get("value", 0.0))
                    except: pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record
        ad_data[ad_id]['purchase_value'] += purchase_value_on_record
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # 크리에이티브 정보 가져오기
    fetch_creatives_parallel(ad_data, ver, token)

    result_list = list(ad_data.values())
    if not result_list: return {"html_table": "<p>Meta 데이터 없음.</p>", "data": []}
    df = pd.DataFrame(result_list)

    # 크리에이티브 컬럼 추가
    # ... (이전과 동일) ...
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '-'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details'])

    # 숫자형 변환
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count', 'purchase_value']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)

    # 계산 지표 생성 (Cafe24 관련 지표 제외)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용 (Meta)'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)
    df['ROAS (Meta)'] = df.apply(lambda r: f"{(r['purchase_value'] / r['spend']):.2f}" if r['spend'] > 0 else '0.00', axis=1)

    # 컬럼 이름 변경 (Cafe24 관련 제외)
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수 (Meta)', 'purchase_value': '구매 전환 값 (Meta)'
    })

    # 합계 행 계산 (Cafe24 관련 제외)
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum()
    total_meta_purchases = df['구매 수 (Meta)'].sum(); total_meta_purchase_value = df['구매 전환 값 (Meta)'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_meta_cpp = int(round(total_spend / total_meta_purchases)) if total_meta_purchases > 0 else 0
    total_meta_roas = f"{(total_meta_purchase_value / total_spend):.2f}" if total_spend > 0 else '0.00'

    # 합계 행 Series (Cafe24 제외)
    totals_data = {
        '광고명': '합계', '캠페인명': '', '광고세트명': '',
        'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks,
        'CTR': total_ctr, 'CPC': total_cpc, '구매 수 (Meta)': total_meta_purchases,
        '구매당 비용 (Meta)': total_meta_cpp, '구매 전환 값 (Meta)': total_meta_purchase_value,
        'ROAS (Meta)': total_meta_roas,
        'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''
    }
    totals_row = pd.Series(totals_data)

    # 컬럼 순서 정의 (Cafe24 제외)
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수 (Meta)', '구매당 비용 (Meta)', '구매 전환 값 (Meta)', 'ROAS (Meta)',
        'ad_id', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df['광고 성과'] = ''
    df = df[[col for col in column_order if col in df.columns]] # 순서 적용

    # 합계 행 추가 및 정렬
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row): # 정렬 로직
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용 (Meta)', 0), errors='coerce')
        return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key', 'display_url', 'target_url'], errors='ignore')

    # 광고 성과 분류
    # ... (이전 코드 유지) ...
    pass # 분류 로직 구현 필요

    # URL 재매핑
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', '')) if 'ad_id' in df_sorted.columns else ''
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', '')) if 'ad_id' in df_sorted.columns else ''

    # HTML 테이블 생성 (Cafe24 컬럼 없음)
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) else "0"
    def format_roas(roas): return f"{float(roas):.2f}" if pd.notna(roas) and str(roas) != '' else "0.00"

    # 기본 Meta 테이블 HTML 생성
    html_table = """
    <style> /* ... CSS ... */ </style>
    <table>
      <thead>
        <tr>
          <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
          <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수 (Meta)</th>
          <th>구매당 비용 (Meta)</th> <th>구매 전환 값 (Meta)</th> <th>ROAS (Meta)</th>
          {/* Cafe24 헤더는 후처리에서 추가됨 */}
          <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
        </tr>
      </thead>
      <tbody>
    """
    iter_df = df_sorted
    for index, row in iter_df.iterrows():
        row_class = 'total-row' if row.get('광고명') == '합계' else ''
        # ... (performance_text, performance_class, content_tag 생성) ...
        performance_text = row.get('광고 성과', '') # 성과 분류 로직 필요
        performance_class = '' # 성과 클래스 지정 필요
        content_tag = "" # 콘텐츠 태그 생성 필요

        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td><td>{row.get('캠페인명','')}</td><td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td><td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td><td>{row.get('CTR','0.00%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td><td>{format_number(row.get('구매 수 (Meta)',0))}</td>
          <td>{format_currency(row.get('구매당 비용 (Meta)',0))}</td><td>{format_currency(row.get('구매 전환 값 (Meta)',0))}</td>
          <td>{format_roas(row.get('ROAS (Meta)',0))}</td>
          {/* Cafe24 데이터 셀은 후처리에서 추가됨 */}
          <td class="{performance_class}">{performance_text}</td><td>{row.get('콘텐츠 유형','-')}</td>
          <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """
    html_table += "</tbody></table>"

    # JSON 데이터 준비
    final_columns_for_json = [col for col in df_sorted.columns if col not in ['ad_id', 'display_url', 'target_url']]
    df_for_json = df_sorted[final_columns_for_json]
    def clean_numeric(data): # NaN/Inf/타입 정리
        # ... (clean_numeric 함수 정의 필요) ...
        pass
    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records) # clean_numeric 호출

    # HTML 테이블과 JSON 데이터 반환 (Cafe24 컬럼 없음)
    return {"html_table": html_table, "data": cleaned_records}


# --- 앱 실행 ---
# if __name__ == '__main__':
#     app.run(debug=True, port=5001)

# clean_numeric 함수 정의 (이전 답변 참고하여 추가)
# ... (clean_numeric 함수 코드) ...
