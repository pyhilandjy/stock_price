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

otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
otp_stk # 이 부분(otp)을 url에 제출하면 데이터를 받을 수 있음.

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
otp_ksq 

down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
down_sector_ksq = rq.post(down_url, {'code': otp_ksq}, headers=headers)
down_sector_ksq.content 

### 클랜징 처리 & 인코딩
sector_ksq = pd.read_csv(BytesIO(down_sector_ksq.content), encoding = 'EUC-KR')

# concat stk, ksq
krx_sector = pd.concat([sector_stk, sector_ksq]).reset_index(drop = True)
krx_sector

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
kor_sector
kor_sector = kor_sector[['IDX_CD', 'CMP_CD', 'CMP_KOR', 'SEC_NM_KOR']]
kor_sector['기준일'] = biz_day
kor_sector['기준일'] = pd.to_datetime(kor_sector['기준일'])

