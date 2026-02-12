from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from pprint import pprint
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import time
import requests
import os
from requests.exceptions import Timeout, ConnectionError, RequestException

#시도 지역
region = [
"1100000000","2600000000","2700000000","2800000000","2900000000","3000000000","3100000000","3600000000","4100000000","4300000000","4400000000","4600000000","4700000000","4800000000","5000000000","5100000000","5200000000"
]
url = 'https://bigdata.kepco.co.kr/openapi/v1/powerUsage/houseAve.do'

# DB에 데이터를 입력하기 위한 engine 생성
def get_engine():
    connect = BaseHook.get_connection('public_data')
    return create_engine(
        f"mysql+pymysql://{connect.login}:{connect.password}@{connect.host}:{connect.port}/{connect.schema}"
    )

#다음으로 조회할 날짜를 구합니다.
def get_target_date():
    """DB에서 최근 날짜 조회, 수집해야 할 달 계산하기"""

    engine = get_engine()
    query = text("""
        SELECT year, month
        FROM household_power
        order by year desc, month desc
        limit 1
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()
    
    year = int(rows[0][0])
    month = int(rows[0][1])

    # 다음 달 계산
    if month==12: 
        year += 1
        month = 1
    else:
        month += 1

    return year, month

# 이어서 다음 date를 탐색하기 위한 다음 날짜 구하기 입니다.
def get_next_date(year, month):
    if month == 12:
        return year+1, 1
    else:
        return year, month+1

# 수집할 새로운 데이터가 나왔는지 확인합니다.
def check_new_data():
    year, month = get_target_date()
    find_list = []

    while True:
        try:
            params = {
                'year' : year,
                'month' : f'{month:02d}',
                'metroCd' : '11',
                'apiKey' : Variable.get("ELECTRIC_API_KEY")
            }
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            data = response.json()
        
            if data and len(data.get('data', [])) > 0:
                print(f"데이터 존재: {len(data['data'])}건")
                find_list.append([year, month])
                year, month = get_next_date(year, month)
                
            
        # 해당 API의 경우 데이터가 없을 경우 404에러를 반환함    
        except Exception as e:
            print(f"데이터 없음: {e}")
            break

    return find_list

# DB의 테이블과 데이터를 맞춰줍니다.
def convert_input(data):
    return {
        'sd_code' : int(data.get('sd_code')),
        'year' : int(data.get('year')),
        'month' : int(data.get('month')),
        'sd_name' : data.get('metro'),
        'sgg_name' : data.get('city'),
        'house_cnt' : int(data.get('houseCnt')),
        'power_usage' : float(data.get('powerUsage')),
        'bill' : int(data.get('bill'))
    }


def save_batch(data_list):
    """배치 데이터 DB 저장 - Raw SQL"""
    if not data_list:
        return
    
    # 실제 컬럼명에 맞게 수정 필요
    query = text("""
        INSERT INTO household_power 
        (sd_code, year, month, sd_name, sgg_name, house_cnt, power_usage, bill)
        VALUES (:sd_code, :year, :month, :sd_name, :sgg_name, :house_cnt, :power_usage, :bill)
    """)
    
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(query, data_list)
        conn.commit()

def is_already_collected(year, month, region_code):
    """이미 수집했는지 확인"""
    query = text("""
        SELECT COUNT(*) as cnt 
        FROM household_power 
        WHERE year=:year AND month=:month AND sd_code=:sd_code
    """)
    
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(query, {'year': year, 'month': month, 'sd_code': region_code})
        return result.fetchone()[0] > 0

def save_failed_items_to_file(fail_list):
    """실패한 항목을 텍스트 파일로 기록"""
    if not fail_list:
        return
    
    # 로그 디렉토리 생성
    log_dir = '../tmp/house_hold_power'
    os.makedirs(log_dir, exist_ok=True)
    
    # 날짜별 파일명
    log_file = f"{log_dir}/failed_{datetime.now().strftime('%Y%m%d')}.txt"
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"\n=== {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        for year, month, region_code in fail_list:
            f.write(f"{year},{month:02d},{region_code}\n")

def get_items(**context):
    """데이터를 수집 합니다."""
    find_list = check_new_data()

    # 새로 추가된 데이터가 없다면
    if not find_list:
        return False
    
    else:
        for date in find_list:
            try_count = 0
            fail_list = []
            year, month = date[0], date[1]

            while try_count < 10:
                if try_count == 0:
                    for r in region:
                        region_code = r[:2]
                        params = {
                            'year' : year,
                            'month' : f'{month:02d}',
                            'metroCd' : region_code,
                            'apiKey' : Variable.get("ELECTRIC_API_KEY")
                        }

                        try:
                            response = requests.get(url, params=params, timeout=5)
                            response.raise_for_status()
                            result = response.json()

                            #row_data에는 제외되어 있는 지역코드를 추가해줍니다.
                            if 'data' in result and result['data']:
                                for data in result['data'] :
                                    data['sd_code'] = region_code

                                #convert_input함수를 통해 dict 리스트의 형태를 테이블과 맞도록 변형해주었습니다.
                                convert_data = [convert_input(d) for d in result['data']]
                                # print(f"{year}-{month:02d} {region_code}: {len(result['data'])}건 입력됨")
                                save_batch(convert_data)  # Raw SQL로 저장
                                    
                        except Exception as e:
                            print(f"✗ {year}-{month:02d} {region_code}: {type(e).__name__}")
                            fail_list.append([year, f'{month:02d}', region_code])
                        
                elif fail_list:
                    new_fail_list = []  # ← 새로운 실패 목록
                    for f in fail_list:
                        params = {
                                'year' : f[0],
                                'month' : f[1],
                                'metroCd' : f[2],
                                'apiKey' : Variable.get("ELECTRIC_API_KEY")
                            }

                        try:
                            response = requests.get(url, params=params, timeout=5)
                            response.raise_for_status()
                            result = response.json()

                            #row_data에는 제외되어 있는 지역코드를 추가해줍니다.
                            if 'data' in result and result['data']:
                                for data in result['data'] :
                                    data['sd_code'] = f[2]

                                #convert_input함수를 통해 dict 리스트의 형태를 테이블과 맞도록 변형해주었습니다.
                                convert_data = [convert_input(d) for d in result['data']]
                                # print(f"{year}-{month:02d} {region_code}: {len(result['data'])}건 입력됨")
                                save_batch(convert_data)  # Raw SQL로 저장
                                    
                        except Exception as e:
                            print(f"✗ {year}-{month:02d} {region_code}: {type(e).__name__}")
                            new_fail_list.append([year, f'{month:02d}', region_code])
                    fail_list = new_fail_list  # ← 업데이트
                else:
                    break

                try_count += 1

            if fail_list:
                save_failed_items_to_file(fail_list)
    
with DAG(
    'household_power_data',
    start_date=datetime(2026, 2, 3),
    schedule='@daily',
    # schedule_interval='0 9 * * *',
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='get_items',
        python_callable=get_items
    )

    task1