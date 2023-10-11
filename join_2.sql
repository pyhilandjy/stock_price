
SELECT * FROM 

select * from team7_database.stockprice_info_re
left join fs_data USING(stock_code);

select thstrm_amount, frmtrm_amount, bfefrmtrm_amount, Se, thstrm, frmtrm, lwfr from team7_database.div
left join fs_data USING(stock_code);
  
  
  select thstrm_amount, bfefrmtrm_amount, se, thstrm, frmtrm, lwfr
  from (SELECT * FROM team7_database.stockprice_info_re AS price
  LEFT JOIN fs_data AS fs USING(stock_code)) AS inlin
  LEFT JOIN team7_database.div USING(stock_code);

CREATE VIEW jun AS 
	select basDt, srtnCd, isinCd, itmsNm, mrktCtg, clpr, vs, fltRt, mkp, hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, sj_div, account_nm, thstrm_amount,frmtrm_amount, bfefrmtrm_amount, se, thstrm, frmtrm, lwfr 
  from (SELECT basDt, srtnCd, isinCd, itmsNm, mrktCtg, clpr, vs, fltRt, mkp, hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, stock_code, sj_div, account_nm, thstrm_amount, frmtrm_amount, bfefrmtrm_amount
  FROM team7_database.stockprice_info_31 AS price
  LEFT JOIN fs_data AS fs ON price.srtnCd = fs.stock_code) AS inlin
  LEFT JOIN team7_database.div USING(stock_code);

SELECT * FROM jun;


insert into jun_table
select * from jun;

  select basDt, srtnCd, isinCd, itmsNm, mrktCtg, clpr, vs, fltRt, mkp, hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, sj_div, account_nm, thstrm_amount,frmtrm_amount, bfefrmtrm_amount, se, thstrm, frmtrm, lwfr 
  from (SELECT basDt, srtnCd, isinCd, itmsNm, mrktCtg, clpr, vs, fltRt, mkp, hipr, lopr, trqu, trPrc, lstgStCnt, mrktTotAmt, stock_code, sj_div, account_nm, thstrm_amount, frmtrm_amount, bfefrmtrm_amount
  FROM team7_database.stockprice_info_31 AS price
  LEFT JOIN fs_data AS fs ON price.srtnCd = fs.stock_code) AS inlin
  LEFT JOIN team7_database.div USING(stock_code)
  
  SELECT
    dart_code.corp_code
  , stockprice_info_31.basDt
  , stockprice_info_31.srtnCd
  , stockprice_info_31.isinCd
  , stockprice_info_31.itmsNm
  , stockprice_info_31.mrktCtg
  , stockprice_info_31.clpr
  , stockprice_info_31.vs
  , stockprice_info_31.fltRt
  , stockprice_info_31.mkp
  , stockprice_info_31.hipr
  , stockprice_info_31.lopr
  , stockprice_info_31.trqu
  , stockprice_info_31.trPrc
  , stockprice_info_31.lstgStCnt
  , stockprice_info_31.mrktTotAmt
  , div.rcept_no
  , div.corp_cls
  , div.corp_code
  , div.corp_name
  , div.se
  , div.thstrm
  , div.frmtrm
  , div.lwfr
  , div.stock_knd
  , div.stock_code
FROM
    dart_code dart_code
    JOIN stockprice_info_31 stockprice_info_31 ON stockprice_info_31.srtnCd = dart_code.stock_code
    JOIN `div` div ON div.corp_code = dart_code.corp_code
  

SELECT * FROM team7_database.stockprice_info_re AS price
  LEFT JOIN team7_database.div AS fs USING(stock_code);
  
  SELECT * FROM team7_database.fs_data AS price
  LEFT JOIN team7_database.div AS fs USING(stock_code);


SELECT * FROM team7_database.stockprice_info_re AS price
  LEFT JOIN fs_data AS fs USING(stock_code);



 select * from team7_database.stockprice_info_re;
 
 
SELECT * FROM (select thstrm_amount, frmtrm_amount, bfefrmtrm_amount, Se, thstrm, frmtrm, lwfr 
from team7_database.div
left join fs_data USING(stock_code)) AS a
inner join team7_database.stockprice_info_re USING(stock_code);


