# api/cafe24_api.py
# -*- coding: utf-8 -*-
import os
import requests
import base64
from datetime import datetime, timedelta, date
import traceback
import time
import json # For logging data structures

# --- Cafe24 설정 로드 (여러 계정 지원) ---
def load_cafe24_configs():
    """환경 변수에서 여러 Cafe24 설정을 로드합니다."""
    configs = {}
    i = 1
    while True:
        name = os.environ.get(f"CAFE24_CONFIG_{i}_NAME")
        mall_id = os.environ.get(f"CAFE24_CONFIG_{i}_MALL_ID")
        client_id = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_ID")
        client_secret = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_SECRET")
        refresh_token = os.environ.get(f"CAFE24_CONFIG_{i}_REFRESH_TOKEN") # Plan B requires Refresh Token

        if name and mall_id and client_id and client_secret and refresh_token:
            config_key = name
            configs[config_key] = {
                "mall_id": mall_id,
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "name": name
            }
            print(f"Loaded Cafe24 config: {name} (Mall ID: {mall_id})")
            i += 1
        else:
            if i == 1 and not name: pass
            elif name or mall_id or client_id or client_secret or refresh_token:
                 print(f"Warning: Incomplete Cafe24 configuration found for index {i}. Requires NAME, MALL_ID, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN. Skipping.")
            break
    if not configs:
        print("Warning: No complete Cafe24 configurations found in environment variables.")
    return configs

# 모듈 로드 시 설정 로드
CAFE24_CONFIGS = load_cafe24_configs()

# 계정별 토큰 캐시 (In-Memory)
# {'config_name': {'token': 'xxx', 'expires_at': datetime_object}}
cafe24_access_token_cache = {}

# --- Cafe24 Access Token 갱신 함수 (계정별 캐시 적용) ---
# (이 함수는 환경 변수의 Refresh Token을 사용합니다)
def get_cafe24_access_token(config_key, config):
    """특정 설정(config_key)에 대한 Access Token을 갱신하고 캐시합니다."""
    global cafe24_access_token_cache

    if not config or not config.get("refresh_token"):
        print(f"Error: Cafe24 refresh token is missing for config '{config_key}'. Cannot get access token.")
        return None

    # 1. Check Cache
    now = datetime.now()
    cache_entry = cafe24_access_token_cache.get(config_key)
    if cache_entry and cache_entry.get("token") and cache_entry.get("expires_at") > now + timedelta(minutes=1): # 1 min buffer
        # print(f"Using cached Cafe24 access token for '{config_key}'.") # Can be verbose
        return cache_entry["token"]

    # 2. Refresh Token if Cache Miss or Expired
    print(f"Attempting to refresh Cafe24 access token for '{config_key}' using Refresh Token...")
    mall_id = config["mall_id"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]
    refresh_token = config["refresh_token"] # 환경 변수에서 로드된 값 사용

    id_secret = f"{client_id}:{client_secret}"
    base64_encoded = base64.b64encode(id_secret.encode()).decode()

    token_url = f"https://{mall_id}.cafe24api.com/api/v2/oauth/token"
    token_data = {
        "grant_type": "refresh_token", # ** Refresh Token 사용 명시 **
        "refresh_token": refresh_token
    }
    token_headers = {
        "Authorization": "Basic " + base64_encoded,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(token_url, data=token_data, headers=token_headers, timeout=15) # Add timeout
        response_data = response.json() # Get JSON regardless of status code initially

        if response.status_code == 200 and "access_token" in response_data:
            new_access_token = response_data["access_token"]
            new_refresh_token = response_data.get("refresh_token") # 응답에 새 Refresh Token이 올 수도 있음
            expires_in = response_data.get("expires_in", 3600) # Default 1 hour

            # Update Cache
            cafe24_access_token_cache[config_key] = {
                "token": new_access_token,
                "expires_at": now + timedelta(seconds=expires_in)
            }
            print(f"Successfully refreshed Cafe24 access token for '{config_key}'.")

            # 새 Refresh Token 처리 (중요)
            if new_refresh_token and new_refresh_token != refresh_token:
                print(f"IMPORTANT: Received a NEW refresh token for '{config_key}'. Update this in your Vercel environment variables!")
                # Vercel 환경 변수는 여기서 직접 수정 불가. 로그 확인 후 수동 업데이트 필요.
                config["refresh_token"] = new_refresh_token # 메모리 상에서는 업데이트 (다음 실행에는 영향 없음)

            return new_access_token
        else:
            print(f"Failed to refresh Cafe24 access token for '{config_key}'. Status: {response.status_code}, Response:")
            print(json.dumps(response_data, indent=2))
            if config_key in cafe24_access_token_cache: # 캐시 비우기
                 del cafe24_access_token_cache[config_key]
            return None # 실패 알림
    except requests.exceptions.RequestException as e:
        print(f"Network error during Cafe24 token refresh request for '{config_key}': {e}")
        if config_key in cafe24_access_token_cache: del cafe24_access_token_cache[config_key]
        return None
    except Exception as e:
        print(f"Unexpected error during Cafe24 token refresh for '{config_key}': {e}")
        traceback.print_exc()
        if config_key in cafe24_access_token_cache: del cafe24_access_token_cache[config_key]
        return None

# --- Cafe24 API 호출 함수들 ---
def get_cafe24_analytics_headers(config_key, config):
    """특정 Cafe24 설정을 위한 Analytics API 헤더 생성"""
    access_token = get_cafe24_access_token(config_key, config) # 토큰 자동 갱신/캐시 처리
    if not access_token:
        print(f"Failed to get/refresh Cafe24 access token for headers (config: '{config_key}').")
        return None

    # === API Version ===
    version = "2025-03-01" # 사용할 API 버전 확인 및 통일
    # ===================
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Cafe24-Api-Version": version,
        "X-Cafe24-Client-Id": config["client_id"] # 데이터 API 호출 시 필요
    }
    return headers

# API 호출 공통 헬퍼 함수 (401 에러 시 자동 재시도 포함)
def _call_cafe24_api(method, url, config_key, config, params=None, data=None, retry_on_401=True):
    """Cafe24 API 호출 (토큰 만료 시 재시도)"""
    headers = get_cafe24_analytics_headers(config_key, config)
    if not headers: return None

    try:
        print(f"Calling Cafe24 API ({method}) for '{config_key}': URL={url}, Params={params}")
        response = requests.request(method, url, headers=headers, params=params, json=data, timeout=25)
        print(f"Cafe24 API ({method}) Status Code for '{config_key}': {response.status_code}")

        # 401 에러 처리 (토큰 만료 가능성)
        if response.status_code == 401 and retry_on_401:
            print(f"Received 401 for '{config_key}'. Clearing cache and retrying token refresh once.")
            if config_key in cafe24_access_token_cache:
                del cafe24_access_token_cache[config_key] # 캐시 삭제

            # 헤더 다시 가져오기 (내부적으로 토큰 갱신 시도)
            headers = get_cafe24_analytics_headers(config_key, config)
            if not headers:
                 print(f"Failed to refresh token after 401 for '{config_key}'. Aborting retry.")
                 return None # 갱신 실패

            print(f"Retrying Cafe24 API ({method}) call for '{config_key}' after token refresh attempt.")
            response = requests.request(method, url, headers=headers, params=params, json=data, timeout=25)
            print(f"Cafe24 API ({method}) Status Code on Retry for '{config_key}': {response.status_code}")

        # 재시도 후 또는 다른 에러 코드 확인
        if response.status_code == 403:
            print(f"Cafe24 API ({method}) 403 Forbidden for '{config_key}'. Check API scopes.")
        elif response.status_code >= 400 :
             print(f"Cafe24 API ({method}) Error {response.status_code} for '{config_key}'.")

        response.raise_for_status() # 4xx/5xx 에러 시 예외 발생
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error calling Cafe24 API ({method}) for '{config_key}': {http_err}")
        if http_err.response is not None: print(f"Response text: {http_err.response.text[:500]}...")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Network error calling Cafe24 API ({method}) for '{config_key}': {req_err}")
        return None
    except Exception as e:
        print(f"Unexpected error in _call_cafe24_api ({method}) for '{config_key}': {e}")
        traceback.print_exc()
        return None

# 특정 API 엔드포인트 호출 함수들
def get_visitors_dailyactive(config_key, config, start_date, end_date):
    """visitors dailyactive API 호출"""
    visitors_url = "https://ca-api.cafe24data.com/visitors/dailyactive"
    params = {"mall_id": config["mall_id"], "start_date": start_date, "end_date": end_date}
    return _call_cafe24_api("GET", visitors_url, config_key, config, params=params)

def get_sales_orderdetails(config_key, config, start_date, end_date):
    """sales orderdetails API 호출 (주의: 페이지네이션 필요할 수 있음)"""
    orderdetails_url = "https://ca-api.cafe24data.com/sales/orderdetails"
    params = {"mall_id": config["mall_id"], "start_date": start_date, "end_date": end_date}
    # 페이지네이션 구현 필요 시 _call_cafe24_api 수정 또는 별도 처리 로직 추가
    return _call_cafe24_api("GET", orderdetails_url, config_key, config, params=params)

# --- Cafe24 데이터 처리 함수 ---
# (이 함수는 API 호출 함수들을 사용하여 일별 데이터를 집계합니다)
def process_cafe24_data(config_key, config, start_date, end_date):
    """특정 Cafe24 설정에 대한 일별 방문자 및 매출 데이터를 가져와 처리합니다."""
    daily_visitors = {}
    daily_sales = {}

    # 1. 일별 방문자 수 가져오기
    print(f"--- Processing Cafe24 Visitors Data for '{config_key}' ---")
    visitors_data = get_visitors_dailyactive(config_key, config, start_date, end_date)
    if visitors_data and 'dailyactive' in visitors_data and isinstance(visitors_data['dailyactive'], list):
        # === 데이터 키 확인 (Cafe24 문서 기준) ===
        visitor_count_key = 'user_count' # 방문자 수 필드
        date_key = 'date'              # 날짜 필드 (YYYY-MM-DDTHH:MM:SS+ZZ:ZZ 형식 예상)
        # =======================================
        processed_visitor_items = 0
        for item in visitors_data['dailyactive']:
            if not isinstance(item, dict): continue
            try:
                date_str = item.get(date_key)
                visitor_count = item.get(visitor_count_key)
                if date_str is None or visitor_count is None: continue

                parsed_date_str = date_str.split('T')[0] # YYYY-MM-DD 부분만 추출
                datetime.strptime(parsed_date_str, '%Y-%m-%d') # 날짜 형식 검증

                daily_visitors[parsed_date_str] = int(visitor_count)
                processed_visitor_items += 1
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                print(f"Error processing visitor item {item} for '{config_key}': {e}")
        print(f"Processed {processed_visitor_items} visitor items for '{config_key}'. Found {len(daily_visitors)} days.")
    else:
        print(f"Could not retrieve or process visitor data for '{config_key}'.")
        if visitors_data: print(f"Received visitors data structure hint: {str(visitors_data)[:500]}...")

    # 2. 일별 주문 금액 가져오기
    print(f"--- Processing Cafe24 Sales Data for '{config_key}' ---")
    # !!! 페이지네이션 처리 필요 시 get_sales_orderdetails 또는 _call_cafe24_api 수정 필요 !!!
    orders_data = get_sales_orderdetails(config_key, config, start_date, end_date)
    if orders_data and 'orderdetails' in orders_data and isinstance(orders_data['orderdetails'], list):
        temp_sales = {}
        # === 데이터 키 확인 (Cafe24 문서 기준) ===
        order_date_key = 'order_date'       # 주문 날짜 필드 (YYYY-MM-DD 형식 예상)
        order_amount_key = 'order_amount'   # 주문 금액 필드 (실제 결제 금액인지 확인 필요)
        order_id_key = 'order_id'         # 주문 번호 (로깅용)
        # =======================================
        order_count_response = len(orders_data['orderdetails'])
        processed_order_items = 0
        for i, order in enumerate(orders_data['orderdetails']):
             if not isinstance(order, dict): continue
             order_date_str = order.get(order_date_key)
             order_amount = order.get(order_amount_key, 0)
             order_id = order.get(order_id_key, f'N/A_idx_{i}')

             if order_date_str:
                 try:
                     datetime.strptime(order_date_str, '%Y-%m-%d') # 날짜 형식 검증
                     date_str = order_date_str

                     current_total = temp_sales.get(date_str, 0)
                     try: amount_int = int(float(order_amount)) # 문자열/소수점 가능성 고려
                     except (ValueError, TypeError): amount_int = 0

                     new_total = current_total + amount_int
                     temp_sales[date_str] = new_total
                     processed_order_items += 1
                 except (ValueError, TypeError) as e:
                  # 로그 너무 많아지는 것 방지
                  if i < 5 or i % 50 == 0:
                     print(f"Skipping order {order_id} for '{config_key}' due to invalid date/amount: {e}")
                  # continue 문의 들여쓰기를 위 if 문과 같은 레벨로 맞춤
                  continue

        daily_sales = temp_sales
        print(f"Processed {processed_order_items} order items from {order_count_response} orders in response for '{config_key}'. Found {len(daily_sales)} days with sales. (Pagination may apply)")
    else:
        print(f"Could not retrieve or process order details data for '{config_key}'.")
        if orders_data: print(f"Received order data structure hint: {str(orders_data)[:500]}...")

    # 날짜 범위 기준으로 0 채우기
    try:
        s_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        e_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if s_date > e_date:
             print(f"Warning: Start date ({start_date}) is after end date ({end_date}) for '{config_key}'.")
             return {"visitors": {}, "sales": {}}

        delta = e_date - s_date
        all_dates = [(s_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]
    except ValueError:
        print(f"Error parsing date range '{start_date}' - '{end_date}' for '{config_key}'.")
        return {"visitors": {}, "sales": {}}

    final_visitors = {d: daily_visitors.get(d, 0) for d in all_dates}
    final_sales = {d: daily_sales.get(d, 0) for d in all_dates}
    print(f"--- Finished processing Cafe24 data for '{config_key}' ---")

    # 일별 데이터 반환
    return {"visitors": final_visitors, "sales": final_sales}
