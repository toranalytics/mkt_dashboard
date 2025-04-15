# api/cafe24_api.py
import os
import requests
import base64
from datetime import datetime, timedelta, date
import traceback # 오류 로깅 추가

# --- Cafe24 설정 로드 ---
def load_cafe24_config():
    config = {
        "mall_id": os.environ.get("CAFE24_MALL_ID"),
        "client_id": os.environ.get("CAFE24_CLIENT_ID"),
        "client_secret": os.environ.get("CAFE24_CLIENT_SECRET"),
        "refresh_token": os.environ.get("CAFE24_REFRESH_TOKEN") # 저장된 리프레시 토큰 로드
    }
    if not all(config.values()):
        print("Warning: Missing one or more Cafe24 configurations in environment variables (CAFE24_MALL_ID, CAFE24_CLIENT_ID, CAFE24_CLIENT_SECRET, CAFE24_REFRESH_TOKEN). Cafe24 API features will be disabled.")
        return None
    print(f"Loaded Cafe24 config for mall: {config['mall_id']}")
    return config

# 모듈 로드 시 설정 로드
CAFE24_CONFIG = load_cafe24_config()

# 간단한 인메모리 토큰 캐시 (더 견고한 방식 고려 가능)
cafe24_access_token_cache = {"token": None, "expires_at": datetime.now()}

# --- Cafe24 Access Token 갱신 함수 ---
def get_cafe24_access_token(config):
    """저장된 Refresh Token을 사용하여 새로운 Access Token을 발급받습니다."""
    global cafe24_access_token_cache

    if not config or not config.get("refresh_token"):
        print("Error: Cafe24 refresh token is missing in config.")
        return None

    # 캐시 확인 (만료 1분 전이면 갱신)
    if cafe24_access_token_cache["token"] and cafe24_access_token_cache["expires_at"] > datetime.now() + timedelta(minutes=1):
        # print("Using cached Cafe24 access token.") # 너무 자주 로깅될 수 있어 주석 처리
        return cafe24_access_token_cache["token"]

    print("Attempting to refresh Cafe24 access token...")
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
            cafe24_access_token_cache["token"] = new_access_token
            cafe24_access_token_cache["expires_at"] = datetime.now() + timedelta(seconds=expires_in)

            print("Successfully refreshed Cafe24 access token.")
            return new_access_token
        else:
            print("Failed to refresh Cafe24 access token. Response:")
            print(response_data)
            cafe24_access_token_cache = {"token": None, "expires_at": datetime.now()} # 캐시 비우기
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during Cafe24 token refresh request: {e}")
        cafe24_access_token_cache = {"token": None, "expires_at": datetime.now()} # 캐시 비우기
        return None
    except Exception as e:
        print(f"Unexpected error during Cafe24 token refresh: {e}")
        traceback.print_exc() # 상세 오류 출력
        cafe24_access_token_cache = {"token": None, "expires_at": datetime.now()} # 캐시 비우기
        return None


# --- Cafe24 API 호출 함수들 ---
def get_cafe24_analytics_headers(config):
    """Cafe24 Analytics API용 헤더 생성"""
    access_token = get_cafe24_access_token(config) # 최신 토큰 가져오기
    if not access_token:
        print("Failed to get Cafe24 access token for headers.")
        return None

    # Cafe24 분석 API 버전 확인 필요 (문서 기준 최신 버전 사용 권장)
    version = "2023-06-01" # 예시, 실제 최신 버전 확인 필요
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Cafe24-Api-Version": version,
        "X-Cafe24-Client-Id": config["client_id"]
    }
    return headers

def get_visitors_dailyactive(config, start_date, end_date):
    """visitors dailyactive API 호출"""
    headers = get_cafe24_analytics_headers(config)
    if not headers: return None

    visitors_url = "https://ca-api.cafe24data.com/visitors/dailyactive"
    params = {"mall_id": config["mall_id"], "start_date": start_date, "end_date": end_date}
    print(f"Calling Cafe24 Visitors API: {visitors_url} with params: {params}")
    try:
        response = requests.get(visitors_url, headers=headers, params=params)
        print(f"Cafe24 Visitors API Status Code: {response.status_code}")
        if response.status_code == 403:
             print(f"Cafe24 Visitors API 403 Forbidden. Check scopes ('mall.read_analytics' required) and token validity.")
             print(f"Response: {response.text[:500]}...") # 응답 일부만 로깅
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling Cafe24 visitors dailyactive API: {e}")
        if response is not None: print(f"Response text: {response.text[:500]}...")
        return None
    except Exception as e:
        print(f"Unexpected error in get_visitors_dailyactive: {e}")
        traceback.print_exc()
        return None

def get_sales_orderdetails(config, start_date, end_date):
    """sales orderdetails API 호출"""
    headers = get_cafe24_analytics_headers(config)
    if not headers: return None

    orderdetails_url = "https://ca-api.cafe24data.com/sales/orderdetails"
    params = {"mall_id": config["mall_id"], "start_date": start_date, "end_date": end_date}
    print(f"Calling Cafe24 OrderDetails API: {orderdetails_url} with params: {params}")
    try:
        response = requests.get(orderdetails_url, headers=headers, params=params)
        print(f"Cafe24 OrderDetails API Status Code: {response.status_code}")
        if response.status_code == 403:
            print(f"Cafe24 OrderDetails API 403 Forbidden. Check scopes ('mall.read_analytics' required) and token validity.")
            print(f"Response: {response.text[:500]}...") # 응답 일부만 로깅
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error calling Cafe24 sales orderdetails API: {e}")
        if response is not None: print(f"Response text: {response.text[:500]}...")
        return None
    except Exception as e:
        print(f"Unexpected error in get_sales_orderdetails: {e}")
        traceback.print_exc()
        return None

# --- Cafe24 데이터 처리 함수 ---
def process_cafe24_data(config, start_date, end_date):
    """지정된 기간의 Cafe24 방문자 및 매출 데이터를 가져와 처리합니다."""
    daily_visitors = {}
    daily_sales = {}

    # 1. 일별 방문자 수 가져오기
    print("--- Processing Cafe24 Visitors Data ---")
    visitors_data = get_visitors_dailyactive(config, start_date, end_date)
    if visitors_data and 'dailyactive' in visitors_data:
        for item in visitors_data['dailyactive']:
            try:
                # 날짜 형식 'YYYY-MM-DD' 로 통일
                dt_obj = datetime.fromisoformat(item['date']).date()
                date_str = dt_obj.strftime('%Y-%m-%d')
                daily_visitors[date_str] = item.get('user_count', 0)
            except Exception as e:
                print(f"Error processing visitor item {item}: {e}")
        print(f"Processed {len(daily_visitors)} days of visitor data.")
    else:
        print("Could not retrieve or process visitor data.")

    # 2. 일별 주문 금액 가져오기
    print("--- Processing Cafe24 Sales Data ---")
    orders_data = get_sales_orderdetails(config, start_date, end_date)
    if orders_data and 'orderdetails' in orders_data:
        temp_sales = {}
        order_count = len(orders_data['orderdetails'])
        for i, order in enumerate(orders_data['orderdetails']):
            order_date_str = order.get('order_date')
            order_amount = order.get('order_amount', 0)
            order_id = order.get('order_id', 'N/A') # 디버깅용 ID
            # print(f"Processing order {i+1}/{order_count}, ID: {order_id}, Date: {order_date_str}, Amount: {order_amount}") # 상세 로깅 필요시
            if order_date_str:
                try:
                     # 날짜 형식 통일 및 유효성 검사
                     dt_obj = datetime.strptime(order_date_str, '%Y-%m-%d').date()
                     date_str = dt_obj.strftime('%Y-%m-%d')
                     # 집계 (누적)
                     current_total = temp_sales.get(date_str, 0)
                     new_total = current_total + int(order_amount)
                     temp_sales[date_str] = new_total
                     # print(f"  Date {date_str}: {current_total} + {int(order_amount)} = {new_total}") # 집계 과정 로깅
                except (ValueError, TypeError) as e:
                     print(f"Skipping order {order_id} due to invalid date ('{order_date_str}') or amount ('{order_amount}'): {e}")
                     continue
        daily_sales = temp_sales
        print(f"Processed {len(daily_sales)} days of sales data from {order_count} orders.")
    else:
        print("Could not retrieve or process order details data.")

    # 날짜 범위를 기준으로 모든 날짜 포함시키기 (0으로 채움)
    try:
        s_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        e_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        delta = e_date - s_date
        all_dates = [(s_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]
    except ValueError:
        print(f"Error parsing date range: {start_date} - {end_date}. Returning empty data.")
        return {"visitors": {}, "sales": {}}


    final_visitors = {d: daily_visitors.get(d, 0) for d in all_dates}
    final_sales = {d: daily_sales.get(d, 0) for d in all_dates}
    print("--- Finished processing Cafe24 data ---")

    return {"visitors": final_visitors, "sales": final_sales}
