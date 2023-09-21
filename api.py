import keyring
import requests as rq
from io import BytesIO
import zipfile
import xmltodict
import json
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from datetime import date
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from bs4 import BeautifulSoup
import re
import time

#save api_key
keyring.set_password('dart_api_key', 'jun', '52a8f2deb975728e8626c2cd94d4af3b0b7fefcb')
dart_api_key = keyring.get_password('dart_api_key', 'jun')
keyring.set_password('price_api_key', 'jun', 'tmS99OrQLxahILsFmG5Q8BhcraMrFghrBWrO1MuJoNbCuoPYUsOrr0rWW3p%2FicigCVcXPFt0Jvfim9ZJYVyw4g%3D%3D')
price_api_key = keyring.get_password('price_api_key', 'jun')
keyring.set_password('div_api_key', 'jun', 'tmS99OrQLxahILsFmG5Q8BhcraMrFghrBWrO1MuJoNbCuoPYUsOrr0rWW3p%2FicigCVcXPFt0Jvfim9ZJYVyw4g%3D%3D')
div_api_key = keyring.get_password('div_api_key', 'jun')
ip = '43.201.116.164'

# connect sql
engine = create_engine(f'mysql+pymysql://jun:12345678@{ip}:3306/quant')
con = pymysql.connect(user='jun',
                      passwd='12345678',
                      host='{ip}',
                      db='quant',
                      charset='utf8')
mycursor = con.cursor()

#crawling

# 추출할 데이터의 날짜
url = 'https://finance.naver.com/sise/sise_deposit.naver'
data = rq.get(url)
data_html = BeautifulSoup(data.content)
parse_day = data_html.select_one(
    'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text
biz_day = re.findall('[0-9]+', parse_day)
biz_day = ''.join(biz_day)


#ticker

## 코스피 (stk)
gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
gen_otp_stk = {
    'locale': 'ko_KR',
    'mktId': 'STK',
    'trdDd': biz_day,
    'money': '1',
    'csvxls_isNo': 'false',
    'name': 'fileDown',
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
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
    'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
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

# 삭제된 종목, 추가된 종목이 있을 수 있어 replace
kor_ticker.to_sql(name='kor_ticker', con=engine, index=True, if_exists='replace')
kor_sector.to_sql(name='kor_sector', con=engine, index=True, if_exists='replace')


# 주가 데이터
# 티커 리스트 불러오기
ticker_list = pd.read_sql("""
                    select * from kor_ticker
                    where 기준일 = (select max(기준일) from kor_ticker) and 종목구분 = '보통주';
                    """, con=engine)                          

# 오류 발생시 저장할 리스트 생성
error_list = []

# 전종목 주가 다운로드 및 저장
for i in tqdm(range(0, len(ticker_list))):
    
    # 티커 선택
    ticker = ticker_list['종목코드'][i]
    
    # 시작일과 종료일
    #fr = (date.today() + relativedelta(years=-5)).strftime('%Y%m%d')
    fr = (date.today() + relativedelta(days = -1)).strftime("%Y%m%d")
    to = (date.today()).strftime('%Y%m%d')
    
    
    # 오류 발생 시 이를 무시하고 다음 루프로 진행
    try:
    
        # url 생성
        url = f'https://api.finance.naver.com/siseJson.naver?symbol={ticker}&requestType=1&startTime={fr}&endTime={to}&timeframe=day'
        
        # 데이터 다운로드
        data = rq.get(url).content
        data_price = pd.read_csv(BytesIO(data))
        
        # 데이터 클랜징
        price = data_price.iloc[:, 0:6]
        price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
        price = price.dropna()
        price['날짜'] = price['날짜'].str.extract('(\d+)')
        price['날짜'] = pd.to_datetime(price['날짜'])
        price['종목코드'] = ticker
        price['종목명'] = ticker_list[ticker_list['종목코드'] == ticker]['종목명'].values[0]

        # 주가 데이터를 DB에 저장
        price.to_sql(name="stock_price", con=engine, index=True, if_exists='append')
        
    except:
        
        # 오류 발생 시 error_list에 티커 저장하고 넘어가기
        print(ticker)
        error_list.append(ticker)
    
    # 타임슬립 적용
    time.sleep(1)


