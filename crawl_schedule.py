# import keyring
import requests as rq
import numpy as np
from io import BytesIO
import zipfile
import xmltodict
import json
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from datetime import date,datetime,timedelta
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from bs4 import BeautifulSoup
import re
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
def dailyCrawl():
    ip = '52.79.207.181'
    print(ip)
    # connect sql
    engine = create_engine(f'mysql+pymysql://team8:1234qwer@{ip}:3306/quant')
    con = pymysql.connect(user='team8',
                        passwd='1234qwer',
                        host= ip,
                        db='quant',
                        charset='utf8')
    mycursor = con.cursor()
    #crawling
    # 추출할 데이터의 날짜
    url = 'https://finance.naver.com/sise/sise_deposit.naver'
    data = rq.get(url)
    data_html = BeautifulSoup(data.content, features="lxml")
    parse_day = data_html.select_one(
        'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text
    biz_day = re.findall('[0-9]+', parse_day)
    biz_day = ''.join(biz_day)

    ## 코스피 (stk)
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'locale': 'ko_KR',
        'mktId': 'STK',
        'trdDd': biz_day,
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03901',
    }
    ### OTP 받기 전 나의 행적을 알려주기
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    # 이 부분(otp)을 url에 제출하면 데이터를 받을 수 있음.
    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    down_sector_stk.content # 받은 csv파일이 html형태로 나타남.
    ### 클랜징 처리 & 인코딩
    sector_stk = pd.read_csv(BytesIO(down_sector_stk.content), encoding = 'EUC-KR')
    ## 코스닥 (ksq)
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_ksq = {
        'locale': 'ko_KR',
        'mktId': 'KSQ',
        'trdDd': biz_day,
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03901',
    }
    ### referer
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}

    otp_ksq = rq.post(gen_otp_url, gen_otp_ksq, headers=headers).text
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
    down_sector_ksq.content

    ### 클랜징 처리 & 인코딩
    sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding = 'EUC-KR')
    # concat stk, ksq
    krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop = True)
    # 종목명 null 데이터 클랜징 (strip) & 기준일 추가.
    krx_sector['종목명'] = krx_sector['종목명'].str.strip()
    krx_sector['기준일'] = biz_day

    # 개별종목 지표 크롤링
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_data = {
        'locale': 'ko_KR',
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': biz_day,
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    otp = rq.post(gen_otp_url, gen_otp_data, headers=headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    krx_ind = rq.post(down_url, {'code': otp}, headers=headers)

    krx_ind = pd.read_csv(BytesIO(krx_ind.content), encoding='EUC-KR')
    krx_ind['종목명'] = krx_ind['종목명'].str.strip()
    krx_ind['기준일'] = biz_day

    # 중복되지 않는 하나의 지표에만 존재하는 종목명 데이터 체크
    set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명']))
    #merge
    kor_ticker = pd.merge(krx_sector,
                          krx_ind,
                          on=krx_sector.columns.intersection(krx_ind.columns).tolist(),
                          how='outer')
    diff = list(set(krx_sector['종목명']).symmetric_difference(set(krx_ind['종목명'])))

    kor_ticker['종목구분'] = np.where(kor_ticker['종목명'].str.contains('스팩|제[0-9]+호'), '스팩',
                                          np.where(kor_ticker['종목코드'].str[-1:] != '0', '우선주',
                                                   np.where(kor_ticker['종목명'].str.endswith('리츠'), '리츠',
                                                            np.where(kor_ticker['종목명'].isin(diff), '기타','보통주'))))

    kor_ticker = kor_ticker.reset_index(drop=True)
    kor_ticker.columns = kor_ticker.columns.str.replace(' ', '') # 컬럼명 공백 제거
    kor_ticker = kor_ticker[['종목코드', '종목명', '시장구분', '종가', '시가총액', '기준일', 'EPS', '선행EPS', 'BPS', '주당배당금', '종목구분']] # 원하는 것만 선택
    kor_ticker = kor_ticker.replace({np.nan: None}) # nan은 SQL에 저장할 수 없으므로 None으로 변경
    print(12345)
    #sector
    url = f'''https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd=G10'''
    data = rq.get(url).json()
    data_pd = pd.json_normalize(data['list'])
    sector_code = [
        'G25', 'G35', 'G50', 'G40', 'G10', 'G20', 'G55', 'G30', 'G15', 'G45'
    ]
    data_sector = []
    for i in tqdm(sector_code):
        url = f'''https://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd={i}'''
        data = rq.get(url).json()
        data_pd = pd.json_normalize(data['list'])

        data_sector.append(data_pd)
        time.sleep(1)
    kor_sector = pd.concat(data_sector, axis = 0)
    kor_sector = kor_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']]
    kor_sector['기준일'] = biz_day
    kor_sector['기준일'] = pd.to_datetime(kor_sector['기준일'])
    #print("asdf")
    # 삭제된 종목, 추가된 종목이 있을 수 있어 replace
    kor_ticker.to_sql(name='kor_ticker', con=engine, index=True, if_exists='replace')
    kor_sector.to_sql(name='kor_sector', con=engine, index=True, if_exists='replace')

    #kor_ticker.to_csv('/home/ubuntu/Crawl/ticker.csv', index=False, mode='w', encoding='utf-8-sig')
    #kor_sector.to_csv('/home/ubuntu/Crawl/sector.csv', index=False, mode='w', encoding='utf-8-sig')
    # 주가 데이터
    # 티커 리스트 불러오기
    ticker_list = pd.read_sql("""
                        select * from kor_ticker
                        where 기준일 = (select max(기준일) from kor_ticker) and 종목구분 = '보통주';
                        """, con=engine)
    # 오류 발생시 저장할 리스트 생성
    error_list_price = []
    # 전종목 주가 다운로드 및 저장
    for i in tqdm(range(0, len(ticker_list))):
        print(i)
        # 티커 선택
        ticker = ticker_list['종목코드'][i]

        # to_sqp
        # 시작일과 종료일
        #fr = (date.today() + relativedelta(years=-5)).strftime('%Y%m%d')
        fr = (date.today() + relativedelta(days = 0)).strftime("%Y%m%d")

        to = (date.today()).strftime('%Y%m%d')
        #print("hi")
        try:
            # url 생성
            url = f'https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1&startTime={fr}&endTime={to}&timeframe=day'
            #print(url)
            # request data
            data = rq.get(url).content
            data_price = pd.read_csv(BytesIO(data))
            # cleansing
            price = data_price.iloc[:, 0:6]
            price.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            price = price.dropna()
            price['date'] = price['date'].str.extract('(\d+)')
            price['date'] = pd.to_datetime(price['date'])
            price['stock_code'] = ticker
            price['stock_name'] = ticker_list[ticker_list['종목코드'] == ticker]['종목명'].values[0]
            #print(price)

            price.to_sql(name="stock_price", con=engine, index=True, if_exists='append')
            #print(i)
            if i == 0:
                price.to_csv(f'/home/ubuntu/Crawl/{to}_output.csv', index=False, mode='w', encoding='utf-8-sig')
            else:
                price.to_csv(f'/home/ubuntu/Crawl/{to}_output.csv', index=False, mode='a', encoding='utf-8-sig', header=False)

        except Exception as e:
            # 오류 발생 시 error_list에 티커 저장하고 넘어가기
            print(e)
            print(f'error on :{ticker}')
            error_list_price.append(ticker)

        # 타임슬립 적용
        time.sleep(1)
    print(f"{i} rows Added on stock_price.")

my_dag = DAG(
    dag_id='crawl_task',
    start_date=datetime(2023, 10, 12),
    schedule_interval="@daily",
)

#def tempo():
#    print("Hello, World!")

task = PythonOperator(
    task_id='crawl_task',
    python_callable=dailyCrawl,
    dag=my_dag,
)


