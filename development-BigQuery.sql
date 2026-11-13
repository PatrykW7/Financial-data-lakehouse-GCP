
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


############## DIM_COMPANY

# CREATE PROCEDURE - SIMPLE 
CREATE OR REPLACE PROCEDURE raw.insert_dimCompany(
  source_link STRING
)
BEGIN 
  DECLARE source_uris ARRAY<STRING> DEFAULT [source_link];
  
  CREATE OR REPLACE EXTERNAL TABLE 
  `gcp-pde-498614.raw.ext_company_facts`
  WITH CONNECTION `gcp-pde-498614.europe-central2.delta_gcs_conn`
  OPTIONS(
    format = "DELTA_LAKE",
    uris = (source_uris)
  );

  CREATE OR REPLACE TEMPORARY TABLE tmp_company_facts AS 
  SELECT DISTINCT CURRENT_TIMESTAMP AS created_at, cik, entityName AS company_name, CURRENT_DATE AS valid_from_date, DATE(9999, 12, 31) AS valid_to_date, True AS is_current FROM `gcp-pde-498614.raw.ext_company_facts`;

  # UPSERT
  MERGE INTO raw.dimCompany tgt USING (
    
    --- INSERT COMPLETELY NEW ROWS
    SELECT src_null.*, CAST(NULL AS INT64) AS merge_key, 'INSERT' AS action FROM tmp_company_facts src_null
    LEFT JOIN raw.dimCompany tgt ON src_null.cik = tgt.cik AND tgt.is_current = True
    WHERE tgt.cik IS NULL OR tgt.company_name <> src_null.company_name

    UNION ALL

    --- UPDATES RECORDS THAT CHANGED IN SOURCE 
    SELECT src.*, src.cik AS merge_key, 'UPDATE' AS action FROM tmp_company_facts src
    JOIN raw.dimCompany tgt ON src.cik = tgt.cik
    WHERE tgt.is_current = True AND tgt.company_name <> src.company_name

    UNION ALL
    
    --- DELETE
    SELECT tgt.created_at, tgt.cik, tgt.company_name, tgt.valid_from_date, tgt.valid_to_date, 
    tgt.is_current, tgt.cik AS merge_key, 'DELETE' AS action FROM raw.dimCompany tgt
    LEFT JOIN tmp_company_facts src ON tgt.cik = src.cik
    WHERE tgt.is_current = True AND src.cik IS NULL 
    
  ) src

  ON tgt.cik = src.merge_key AND tgt.is_current = True

  WHEN MATCHED AND (tgt.company_name <> src.company_name) OR src.action = 'DELETE' THEN 
  UPDATE SET 
    valid_to_date = src.valid_from_date,
    is_current = False

  WHEN NOT MATCHED THEN
  INSERT (created_at, cik, company_name, valid_from_date, valid_to_date, is_current)
  VALUES (src.created_at, src.cik, src.company_name, src.valid_from_date, src.valid_to_date, src.is_current);
END;




CALL raw.insert_dimCompany(
  'gs://project-dev-storage/silver/company_facts/2026-11-01'
);

SELECT * FROM raw.tmp_company_facts

--SELECT * FROM raw.dimCompany


SELECT * FROM raw.dimCompany 


SELECT CAST(NULL AS INT64)

--CREATE OR REPLACE TABLE raw.dimCompany AS SELECT * FROM raw.dimCompany_kopia

INSERT INTO raw.dimCompany (created_at, cik, company_name, valid_from_date, valid_to_date, is_current)
VALUES (CURRENT_TIMESTAMP, 432534, 'MSC', CURRENT_DATE, DATE(9999,12,31), True)

ALTER TABLE raw.dimCompany ALTER COLUMN DWHIdCompany SET GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1)



############## DIM_TICKER
SELECT ddl
FROM `gcp-pde-498614.raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'dimCompany';


CREATE OR REPLACE TABLE raw.dimTicker(
  DWHIdTicker INT64 GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
  created_at TIMESTAMP,
  cik STRING,
  ticker STRING,
  exchange STRING,
  valid_from_date DATE,
  valid_to_date DATE,
  is_current BOOL
)
;

CREATE OR REPLACE EXTERNAL TABLE 
`gcp-pde-498614.raw.ext_company_tickers`
OPTIONS(
  format = "DELTA_LAKE",
  uris = ["gs://project-dev-storage/staging/company_tickers/companyTickers_stg_2026-10-31"]
);


SELECT * FROM `gcp-pde-498614.raw.ext_company_tickers`



CREATE OR REPLACE PROCEDURE raw.insert_dimTicker()
BEGIN
# BIGLAKE TABLE
  CREATE OR REPLACE EXTERNAL TABLE 
  `gcp-pde-498614.raw.ext_company_tickers`
  WITH CONNECTION `gcp-pde-498614.europe-central2.delta_gcs_conn`
  OPTIONS(
    format = "DELTA_LAKE",
    uris = ["gs://project-dev-storage/staging/company_tickers/companyTickers_stg_2026-10-31"]
  );


  CREATE OR REPLACE TEMPORARY TABLE tmp_company_tickers AS
  SELECT CURRENT_TIMESTAMP AS created_at, cik, ticker, exchange, CURRENT_DATE() AS valid_from_date, DATE(9999,12,31) AS valid_to_date, True AS is_current
  FROM `gcp-pde-498614.raw.ext_company_tickers`;


  MERGE INTO `gcp-pde-498614.raw.dimTicker` tgt
  USING (
    -- INSERT NEW ROWS
    SELECT src.*, 'NULL' AS merge_key, 'INSERT' AS action FROM tmp_company_tickers src
    LEFT JOIN `gcp-pde-498614.raw.dimTicker` tgt ON src.cik = tgt.cik AND tgt.is_current = True 
    WHERE tgt.cik IS NULL OR (src.ticker <> tgt.ticker OR src.exchange <> tgt.exchange)

    UNION ALL

    -- UPDATE ROW 
    SELECT src.*, src.cik AS merge_key, 'UPDATE' AS action FROM tmp_company_tickers src
    INNER JOIN `gcp-pde-498614.raw.dimTicker` tgt ON src.cik = tgt.cik 
    WHERE tgt.is_current = TRUE
    AND   (src.ticker <> tgt.ticker
    OR    src.exchange <> tgt.exchange
          )

    UNION ALL

    -- DELETED ROWS
    SELECT tgt.created_at, tgt.cik, tgt.ticker, tgt.exchange, tgt.valid_from_date, tgt.valid_to_date, tgt.is_current, tgt.cik AS merge_key, 'DELETE' AS action
    FROM `gcp-pde-498614.raw.dimTicker` tgt 
    LEFT JOIN tmp_company_tickers src ON tgt.cik = src.cik 
    WHERE src.cik IS NULL AND tgt.is_current = True
    
  ) src

  ON src.merge_key = tgt.cik AND tgt.is_current = True

  WHEN MATCHED AND src.action IN ('UPDATE','DELETE') THEN
  UPDATE SET 
    tgt.valid_to_date = src.valid_from_date,
    tgt.is_current = False
  
  
  WHEN NOT MATCHED THEN 
  INSERT (created_at, cik, ticker, exchange, valid_from_date, valid_to_date, is_current)
  VALUES (src.created_at, src.cik, src.ticker, src.exchange, src.valid_from_date, src.valid_to_date, src.is_current);
  
END;


SELECT * FROM `gcp-pde-498614.raw.dimTicker`


CALL raw.insert_dimTicker();

SELECT ddl FROM `gcp-pde-498614.raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'dimTicker'





