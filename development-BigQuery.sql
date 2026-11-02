
# TABLE CREATE - first dm.dimCompany
CREATE OR REPLACE TABLE raw.dimCompany
(
  DWHIdCompany INT64 GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
  created_at TIMESTAMP,
  cik INT64,
  company_name STRING,
  valid_from_date DATE,
  valid_to_date DATE,
  is_current BOOL
);


###
# CREATE CONNECTION 
CREATE CONNECTION IF NOT EXISTS
  `gcp-pde-498614.europe-central2.delta_gcs_conn`
OPTIONS (
  connection_type = 'CLOUD_RESOURCE',
  friendly_name = 'Delta Lake GCS Connection'
);





# CREATE BIGLAKE TABLE 
CREATE OR REPLACE EXTERNAL TABLE 
`gcp-pde-498614.raw.ext_company_facts`
WITH CONNECTION `gcp-pde-498614.europe-central2.delta_gcs_conn`
OPTIONS(
  format = "DELTA_LAKE",
  uris = ["gs://project-dev-storage/silver/company_facts/2026-10-31"]
);


# CREATE PROCEDURE - SIMPLE 
CREATE OR REPLACE PROCEDURE raw.insert_dimCompany()
BEGIN 
  CREATE OR REPLACE EXTERNAL TABLE 
  `gcp-pde-498614.raw.ext_company_facts`
  WITH CONNECTION `gcp-pde-498614.europe-central2.delta_gcs_conn`
  OPTIONS(
    format = "DELTA_LAKE",
    uris = ["gs://project-dev-storage/silver/company_facts/2026-11-01"]
  );

  CREATE OR REPLACE TEMPORARY TABLE tmp_company_facts AS 
  SELECT cik, entityName FROM `gcp-pde-498614.raw.ext_company_facts`;
  /*
  INSERT INTO raw.dimCompany (created_at, cik, company_name, valid_from_date, valid_to_date, is_current) SELECT DISTINCT CURRENT_TIMESTAMP(), cik, entityName, CURRENT_DATE(), DATE(9999,12,31), TRUE
  FROM `gcp-pde-498614.raw.ext_company_facts`;
  */
END;


CALL raw.insert_dimCompany();

SELECT * FROM tmp_company_facts

SELECT * FROM raw.dimCompany

SELECT * FROM tmp_company_facts

--ALTER TABLE raw.dimCompany ADD COLUMN id_deleted BOOL

--UPDATE raw.dimCompany SET id_deleted = False WHERE DWHIdCompany = 27115

--TRUNCATE TABLE raw.dimCompany

SELECT * FROM `gcp-pde-498614.raw.ext_company_facts` LIMIT 100;


SELECT DATE(9999,12,31)

-------------------------------------------


SELECT * FROM `gcp-pde-498614.raw.ext_company_facts` LIMIT 10;


SELECT * FROM raw.dimCompany







