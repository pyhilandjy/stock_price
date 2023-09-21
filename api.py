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

#clawling

#ticker
url = 'https://finance.naver.com/sise/sise_deposit.naver'
data = rq.get(url)
data_html = BeautifulSoup(data.content)
parse_day = data_html.select_one(
    'div.subtop_sise_graph2 > ul.subtop_chart_note > li > span.tah').text
biz_day = re.findall('[0-9]+', parse_day)
biz_day = ''.join(biz_day)








#sector

