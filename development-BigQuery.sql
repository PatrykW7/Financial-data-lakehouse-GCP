
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

  WHEN MATCHED AND src.action IN ('UPDATE','DELETE') THEN 
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
  cik INT64,
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



CREATE OR REPLACE PROCEDURE raw.insert_dimTicker(
  source_link STRING # "gs://project-dev-storage/staging/company_tickers/companyTickers_stg_2026-10-31"
)
BEGIN
  DECLARE source_uris ARRAY<STRING> DEFAULT [source_link];
# BIGLAKE TABLE
  CREATE OR REPLACE EXTERNAL TABLE 
  `gcp-pde-498614.raw.ext_company_tickers`
  WITH CONNECTION `gcp-pde-498614.europe-central2.delta_gcs_conn`
  OPTIONS(
    format = "DELTA_LAKE",
    uris = (source_uris)
  );


  CREATE OR REPLACE TEMPORARY TABLE tmp_company_tickers AS
  SELECT CURRENT_TIMESTAMP AS created_at, CAST(cik AS INT64) AS cik, ticker, exchange, CURRENT_DATE() AS valid_from_date, DATE(9999,12,31) AS valid_to_date, True AS is_current
  FROM `gcp-pde-498614.raw.ext_company_tickers`;


  MERGE INTO `gcp-pde-498614.raw.dimTicker` tgt
  USING (
    -- INSERT NEW ROWS
    SELECT src.*, CAST(NULL AS INT64) AS merge_key, 'INSERT' AS action FROM tmp_company_tickers src
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


CALL raw.insert_dimTicker(
  'gs://project-dev-storage/staging/company_tickers/companyTickers_stg_2026-10-31'
);


SELECT * FROM `gcp-pde-498614.raw.dimTicker`


SELECT ddl FROM `gcp-pde-498614.raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'dimTicker'


############## DIM_FILING
/*
SELECT ddl
FROM `gcp-pde-498614.raw.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'dimTicker'

SELECT * FROM raw.dimTicker
*/


CREATE OR REPLACE EXTERNAL TABLE raw.ext_dimFiling
OPTIONS(
  format = "DELTA_LAKE",
  uris = ["gs://project-dev-storage/silver/company_forms/2026-10-31"]
)


SELECT * FROM raw.ext_dimFiling


CREATE OR REPLACE TABLE raw.dimFiling (
  DWHIdFiling INT64 GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1),
  cik INT64,
  accession_number STRING,
  form_type STRING,
  primary_document STRING,
  is_xbrl INT64,
  is_inlinexbrl INT64,
  is_amendment INT64,
  valid_from_date DATE,
  valid_to_date DATE,
  is_current BOOL
  
)

CLUSTER BY cik;



CREATE OR REPLACE PROCEDURE raw.insert_dimFiling()
BEGIN
  CREATE OR REPLACE EXTERNAL TABLE raw.ext_dimFiling
  WITH CONNECTION `gcp-pde-498614.europe-central2.delta_gcs_conn`
  OPTIONS(
  format = "DELTA_LAKE",
  uris = ["gs://project-dev-storage/silver/company_forms/2026-10-31"]
  );

  CREATE OR REPLACE TEMPORARY TABLE temp_company_forms AS 
  SELECT CAST(cik AS INT64) cik, accessionNumber, form, primaryDocument, isXBRL, isinlineXBRL, CASE WHEN form LIKE '%/A' THEN 1 ELSE 0 END AS is_amendment, CURRENT_DATE() AS valid_from_date, DATE(9999,12, 31) AS valid_to_date, True AS is_current
  FROM raw.ext_dimFiling;


## MERGE INTO raw.dimFiling
MERGE INTO raw.dimFiling tgt USING (
## INSERT NEW ROWS
SELECT src.*, NULL AS merge_key, 'INSERT' AS action FROM temp_company_forms src
LEFT JOIN raw.dimFiling tgt ON (src.cik = tgt.cik AND src.accessionNumber = tgt.accession_number) AND tgt.is_current = True
WHERE tgt.cik IS NULL OR 
(
      src.accessionNumber <> tgt.accession_number OR 
      src.form <> tgt.form_type OR 
      src.primaryDocument <> tgt.primary_document OR
      src.isXBRL <> tgt.is_xbrl OR 
      src.isInlineXBRL <> tgt.is_inlinexbrl OR
      src.is_amendment <> tgt.is_amendment
    )

UNION ALL

## UPDATE ROWS 
SELECT src.*, CONCAT(src.cik, src.accessionNumber) AS merge_key, 'UPDATE' AS action FROM temp_company_forms src
INNER JOIN raw.dimFiling tgt ON (src.cik = tgt.cik AND src.accessionNumber = tgt.accession_number)
WHERE tgt.is_current = True AND (
      src.accessionNumber <> tgt.accession_number OR 
      src.form <> tgt.form_type OR 
      src.primaryDocument <> tgt.primary_document OR
      src.isXBRL <> tgt.is_xbrl OR 
      src.isInlineXBRL <> tgt.is_inlinexbrl OR
      src.is_amendment <> tgt.is_amendment
    )

UNION ALL

## DELETED ROWS
SELECT tgt.cik, tgt.accession_number, tgt.form_type, tgt.primary_document, tgt.is_xbrl, tgt.is_inlinexbrl, tgt.is_amendment, tgt.valid_from_date, tgt.valid_to_date, tgt.is_current, CONCAT(tgt.cik, tgt.accession_number) AS merge_key, 'DELETE' AS action
FROM raw.dimFiling tgt
LEFT JOIN temp_company_forms src ON (src.cik = tgt.cik AND src.accessionNumber = tgt.accession_number)
WHERE src.cik IS NULL AND tgt.is_current = TRUE

) src ON CONCAT(tgt.cik, tgt.accession_number) = src.merge_key AND tgt.is_current = True

WHEN MATCHED AND src.action IN ('UPDATE','DELETE') THEN
UPDATE SET
  tgt.is_current = False,
  tgt.valid_to_date = CURRENT_DATE()


WHEN NOT MATCHED
THEN INSERT (cik, accession_number, form_type, primary_document, is_xbrl, is_inlinexbrl, is_amendment, valid_from_date, valid_to_date, is_current)
VALUES (src.cik, src.accessionNumber, src.form, src.primaryDocument, src.isXBRL, src.isinlineXBRL, src.is_amendment, src.valid_from_date, src.valid_to_date, src.is_current);


END;  


CALL raw.insert_dimFiling();



SELECT * FROM raw.dimFiling

SELECT COUNT(*) FROM raw.dimFiling 


SELECT * FROM raw.ext_dimFiling


SELECT cik, accession_number, form_type, primary_document, is_xbrl, is_inlinexbrl, is_amendment, valid_from_date, valid_to_date, is_current, COUNT(*) 
FROM raw.dimFiling
GROUP BY cik, accession_number, form_type, primary_document, is_xbrl, is_inlinexbrl, is_amendment, valid_from_date, valid_to_date, is_current
HAVING COUNT(*) > 1



SELECT cik, accession_number, COUNT(*) FROM raw.dimFiling
GROUP BY cik, accession_number
HAVING COUNT(*) > 1



