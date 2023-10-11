use stock_db;

SELECT dart_code.stock_code
FROM stock_db.fs
LEFT JOIN stock_db.dart_code ON fs.corp_code = dart_code.corp_code;


SELECT *
FROM stock_db.fs;

ALTER TABLE stock_db.fs
DROP COLUMN thstrm_add_amount;

ALTER TABLE stock_db.fs
ADD COLUMN stock_code VARCHAR(255);

UPDATE stock_db.fs AS fs
LEFT JOIN stock_db.dart_code AS dart_code ON fs.corp_code = dart_code.corp_code
SET fs.stock_code = dart_code.stock_code;

