--create table store raw data
SET search_path =online_retail;
DROP TABLE IF EXISTS online_retail.raw_country;
CREATE TABLE IF NOT EXISTS online_retail.raw_country(
	id 			INTEGER PRIMARY KEY,
	iso 		CHARACTER(50),
	name 		CHARACTER(50),
	nicename 	CHARACTER(50),
	iso3 		CHARACTER(50),
	numcode 	INTEGER,
	phonecode 	INTEGER	
);

DROP TABLE IF EXISTS online_retail.raw_invoices;
CREATE TABLE IF NOT EXISTS online_retail.raw_invoices(
	invoiceno 	CHARACTER(50),
	stockcode 	CHARACTER(50),
	description CHARACTER(50),
	quantity 	INTEGER,
	invoicedate DATE,
	unitprice 	NUMERIC,
	customerid	INTEGER, 
	country 	CHARACTER(50),
	
);
