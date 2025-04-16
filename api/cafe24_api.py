# api/cafe24_api.py
import os
import requests
import base64
from datetime import datetime, timedelta, date
import traceback
import time # 캐시 키 생성 시간 비교용

# --- Cafe24 설정 로드 (여러 계정 지원) ---
def load_cafe24_configs():
    """환경 변수에서 여러 Cafe24 설정을 로드합니다."""
    configs = {}
    i = 1
    while True:
        # 설정 이름으로 매칭하기 위해 NAME 필드 필수
        name = os.environ.get(f"CAFE24_CONFIG_{i}_NAME")
        mall_id = os.environ.get(f"CAFE24_CONFIG_{i}_MALL_ID")
        client_id = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_ID")
        client_secret = os.environ.get(f"CAFE24_CONFIG_{i}_CLIENT_SECRET")
        refresh_token = os.environ.get(f"CAFE24_CONFIG_{i}_REFRESH_TOKEN")

        # 필수 값들이 모두 있는지 확인 (NAME, MALL_ID, CLIENT_ID, SECRET, REFRESH_TOKEN)
        if name and mall_id and client_id and client_secret and refresh_token:
            config_key = name # 설정을 구분하는 키로 NAME 사용
            configs[config_key] = {
                "mall_id": mall_id,
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "name": name # 필요시 내부에서도 이름 사용
            }
            print(f"Loaded Cafe24 config: {name} (Mall ID: {mall_id})")
            i += 1
        else:
            # 하나라도 없으면 더 이상 설정이 없는 것으로 간주하고 중단
            if i == 1 and not name: # 첫 번째 설정 이름조차 없으면 바로 중단
                 pass
            elif name or mall_id or client_id or client_secret or refresh_token: # 일부만 있으면 경고
                 print(f"Warning: Incomplete Cafe24 configuration found for index {i}. Skipping.")
            break # 다음 인덱스로 넘어가지 않음
    if not configs:
        print("Warning: No complete Cafe24 configurations found in environment variables.")
    return configs

def process_cafe24_data(config_key, config, start_date, end_date):
    """
    지정된 기간의 Cafe24 총 방문자 수와 총 매출액을 계산하여 반환합니다.
    """
    total_visitors = 0
    total_sales = 0

    # 1. 총 방문자 수 계산
    print(f"--- Calculating Total Cafe24 Visitors for '{config_key}' ---")
    visitors_data = get_visitors_dailyactive(config_key, config, start_date, end_date)
    if visitors_data and 'dailyactive' in visitors_data:
        for item in visitors_data['dailyactive']:
            total_visitors += item.get('user_count', 0)
        print(f"Calculated total visitors: {total_visitors}")
    else:
        print(f"Could not retrieve visitor data for '{config_key}'. Total visitors set to 0.")

    # 2. 총 매출액 계산
    print(f"--- Calculating Total Cafe24 Sales for '{config_key}' ---")
    orders_data = get_sales_orderdetails(config_key, config, start_date, end_date)
    if orders_data and 'orderdetails' in orders_data:
        current_total_sales = 0
        order_count = len(orders_data['orderdetails'])
        for order in orders_data['orderdetails']:
            order_amount = order.get('order_amount', 0)
            try:
                # 금액 형변환 시도
                current_total_sales += int(float(order_amount))
            except (ValueError, TypeError) as e:
                 order_id = order.get('order_id', 'N/A')
                 print(f"Warning: Invalid order amount '{order_amount}' for order {order_id}. Skipping.")
                 continue
        total_sales = current_total_sales
        print(f"Calculated total sales from {order_count} orders: {total_sales}")
    else:
        print(f"Could not retrieve order details data for '{config_key}'. Total sales set to 0.")

    print(f"--- Finished calculating Cafe24 totals for '{config_key}' ---")

    return {"total_visitors": total_visitors, "total_sales": total_sales}

# 모듈 로드 시 설정 로드
CAFE24_CONFIGS = load_cafe24_configs()

# 계정별 토큰 캐시 (딕셔너리 형태)
# {'config_name': {'token': 'xxx', 'expires_at': datetime_object}}
cafe24_access_token_cache = {}

# --- Cafe24 Access Token 갱신 함수 (계정별 캐시 적용) ---
def get_cafe24_access_token(config_key, config):
    """특정 설정(config_key)에 대한 Access Token을 갱신하고 캐시합니다."""
    global cafe24_access_token_cache

    if not config or not config.get("refresh_token"):
        print(f"Error: Cafe24 refresh token is missing for config '{config_key}'.")
        return None

    # 캐시 확인
    now = datetime.now()
    cache_entry = cafe24_access_token_cache.get(config_key)
    if cache_entry and cache_entry.get("token") and cache_entry.get("expires_at") > now + timedelta(minutes=1):
        # print(f"Using cached Cafe24 access token for '{config_key}'.") # 로그 너무 많아질 수 있음
        return cache_entry["token"]

    print(f"Attempting to refresh Cafe24 access token for '{config_key}'...")
    mall_id = config["mall_id"]
    client_id = config["client_id"]
    client_secret = config["client_secret"]
    refresh_token = config["refresh_token"]

    id_secret = f"{client_id}:{client_secret}"
    base64_encoded = base64.b64encode(id_secret.encode()).decode()

    token_url = f"https://{mall_id}.cafe24api.com/api/v2/oauth/token"
    token_data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }
    token_headers = {
        "Authorization": "Basic " + base64_encoded,
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(token_url, data=token_data, headers=token_headers)
        response.raise_for_status()
        response_data = response.json()

        if "access_token" in response_data:
            new_access_token = response_data["access_token"]
            expires_in = response_data.get("expires_in", 3600) # 기본 1시간

            # 캐시 업데이트
            cafe24_access_token_cache[config_key] = {
                "token": new_access_token,
                "expires_at": now + timedelta(seconds=expires_in)
            }
            print(f"Successfully refreshed Cafe24 access token for '{config_key}'.")
            return new_access_token
        else:
            print(f"Failed to refresh Cafe24 access token for '{config_key}'. Response:")
            print(response_data)
            if config_key in cafe24_access_token_cache: # 해당 키 캐시만 비우기
                 del cafe24_access_token_cache[config_key]
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during Cafe24 token refresh request for '{config_key}': {e}")
        if config_key in cafe24_access_token_cache: del cafe24_access_token_cache[config_key]
        return None
    except Exception as e:
        print(f"Unexpected error during Cafe24 token refresh for '{config_key}': {e}")
        traceback.print_exc()
        if config_key in cafe24_access_token_cache: del cafe24_access_token_cache[config_key]
        return None

# --- Cafe24 API 호출 함수들 (config_key, config 인자 사용) ---
def get_cafe24_analytics_headers(config_key, config):
    """특정 Cafe24 설정을 위한 Analytics API 헤더 생성"""
    access_token = get_cafe24_access_token(config_key, config) # 해당 설정의 토큰 가져오기
    if not access_token:
        print(f"Failed to get Cafe24 access token for headers (config: '{config_key}').")
        return None

    # Cafe24 분석 API 버전 확인 필요
    version = "2023-06-01" # 예시, 실제 최신 버전 확인 필요
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Cafe24-Api-Version": version,
        "X-Cafe24-Client-Id": config["client_id"]
    }
    return headers

def get_visitors_dailyactive(config_key, config, start_date, end_date):
    """특정 몰의 visitors dailyactive API 호출"""
    headers = get_cafe24_analytics_headers(config_key, config)
    if not headers: return None

    visitors_url = "https://ca-api.cafe24data.com/visitors/dailyactive"
    params = {"mall_id": config["mall_id"], "start_date": start_date, "end_date": end_date}
    print(f"Calling Cafe24 Visitors API for '{config_key}': {visitors_url} with params: {params}")
    try:
        response = requests.get(visitors_url, headers=headers, params=params)
        print(f"Cafe24 Visitors API Status Code for '{config_key}': {response.status_code}")
        if response.status_code == 403:
             print(f"Cafe24 Visitors API 403 Forbidden for '{config_key}'. Check scopes and token validity.")
             print(f"Response: {response.text[:500]}...")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling Cafe24 visitors dailyactive API for '{config_key}': {e}")
        if response is not None: print(f"Response text: {response.text[:500]}...")
        return None
    except Exception as e:
        print(f"Unexpected error in get_visitors_dailyactive for '{config_key}': {e}")
        traceback.print_exc()
        return None

def get_sales_orderdetails(config_key, config, start_date, end_date):
    """특정 몰의 sales orderdetails API 호출"""
    headers = get_cafe24_analytics_headers(config_key, config)
    if not headers: return None

    orderdetails_url = "https://ca-api.cafe24data.com/sales/orderdetails"
    params = {"mall_id": config["mall_id"], "start_date": start_date, "end_date": end_date}
    print(f"Calling Cafe24 OrderDetails API for '{config_key}': {orderdetails_url} with params: {params}")
    try:
        response = requests.get(orderdetails_url, headers=headers, params=params)
        print(f"Cafe24 OrderDetails API Status Code for '{config_key}': {response.status_code}")
        if response.status_code == 403:
            print(f"Cafe24 OrderDetails API 403 Forbidden for '{config_key}'. Check scopes and token validity.")
            print(f"Response: {response.text[:500]}...")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling Cafe24 sales orderdetails API for '{config_key}': {e}")
        if response is not None: print(f"Response text: {response.text[:500]}...")
        return None
    except Exception as e:
        print(f"Unexpected error in get_sales_orderdetails for '{config_key}': {e}")
        traceback.print_exc()
        return None

# --- Cafe24 데이터 처리 함수 (config_key, config 인자 사용) ---
def process_cafe24_data(config_key, config, start_date, end_date):
    """특정 Cafe24 설정에 대한 방문자 및 매출 데이터를 가져와 처리합니다."""
    daily_visitors = {}
    daily_sales = {}

    # 1. 일별 방문자 수 가져오기
    print(f"--- Processing Cafe24 Visitors Data for '{config_key}' ---")
    visitors_data = get_visitors_dailyactive(config_key, config, start_date, end_date)
    if visitors_data and 'dailyactive' in visitors_data:
        for item in visitors_data['dailyactive']:
            try:
                dt_obj = datetime.fromisoformat(item['date']).date()
                date_str = dt_obj.strftime('%Y-%m-%d')
                daily_visitors[date_str] = item.get('user_count', 0)
            except Exception as e:
                print(f"Error processing visitor item {item} for '{config_key}': {e}")
        print(f"Processed {len(daily_visitors)} days of visitor data for '{config_key}'.")
    else:
        print(f"Could not retrieve or process visitor data for '{config_key}'.")

    # 2. 일별 주문 금액 가져오기
    print(f"--- Processing Cafe24 Sales Data for '{config_key}' ---")
    orders_data = get_sales_orderdetails(config_key, config, start_date, end_date)
    if orders_data and 'orderdetails' in orders_data:
        temp_sales = {}
        order_count = len(orders_data['orderdetails'])
        for i, order in enumerate(orders_data['orderdetails']):
            order_date_str = order.get('order_date')
            order_amount = order.get('order_amount', 0)
            order_id = order.get('order_id', 'N/A')
            if order_date_str:
                try:
                     dt_obj = datetime.strptime(order_date_str, '%Y-%m-%d').date()
                     date_str = dt_obj.strftime('%Y-%m-%d')
                     current_total = temp_sales.get(date_str, 0)
                     # order_amount가 문자열일 수 있으므로 형변환 및 오류 처리 강화
                     try:
                         amount_int = int(float(order_amount)) # 소수점 가능성 고려 float 후 int
                     except (ValueError, TypeError):
                         amount_int = 0
                         print(f"Warning: Invalid order amount '{order_amount}' for order {order_id}. Using 0.")
                     new_total = current_total + amount_int
                     temp_sales[date_str] = new_total
                except (ValueError, TypeError) as e:
                     print(f"Skipping order {order_id} for '{config_key}' due to invalid date ('{order_date_str}'): {e}")
                     continue
        daily_sales = temp_sales
        print(f"Processed {len(daily_sales)} days of sales data from {order_count} orders for '{config_key}'.")
    else:
        print(f"Could not retrieve or process order details data for '{config_key}'.")

    # 날짜 범위 기준으로 0 채우기
    try:
        s_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        e_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        delta = e_date - s_date
        all_dates = [(s_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]
    except ValueError:
        print(f"Error parsing date range for '{config_key}': {start_date} - {end_date}. Returning empty data.")
        return {"visitors": {}, "sales": {}}

    final_visitors = {d: daily_visitors.get(d, 0) for d in all_dates}
    final_sales = {d: daily_sales.get(d, 0) for d in all_dates}
    print(f"--- Finished processing Cafe24 data for '{config_key}' ---")

    return {"visitors": final_visitors, "sales": final_sales}
