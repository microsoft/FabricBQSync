DECLARE integration_test_schema STRING DEFAULT 'IntegrationTest';
--SET @@dataset_project_id = '';

BEGIN
  DECLARE alt_integration_test_schema STRING DEFAULT CONCAT(integration_test_schema, '_2');

	EXECUTE IMMEDIATE
    CONCAT("CREATE SCHEMA IF NOT EXISTS ", integration_test_schema);

  EXECUTE IMMEDIATE
    CONCAT("CREATE SCHEMA IF NOT EXISTS ", alt_integration_test_schema);

  SET @@dataset_id = integration_test_schema;

	CREATE OR REPLACE TABLE Base_Table_Heap1
	(
	  SaleKey INT64,
	  CityKey INT64,
	  CustomerKey INT64,
	  BillToCustomerKey INT64,
	  StockItemKey INT64,
	  InvoiceDateKey TIMESTAMP,
	  DeliveryDateKey TIMESTAMP,
	  SalespersonKey INT64,
	  WWIInvoiceID INT64,
	  Description STRING,
	  Package STRING,
	  Quantity INT64,
	  UnitPrice NUMERIC,
	  TaxRate NUMERIC,
	  TotalExcludingTax NUMERIC,
	  TaxAmount NUMERIC,
	  Profit NUMERIC,
	  TotalIncludingTax NUMERIC,
	  TotalDryItems INT64,
	  TotalChillerItems INT64,
	  LineageKey INT64
	);

	CREATE OR REPLACE TABLE Base_Table_Heap2 AS
	  SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Heap3 AS
	  SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Heap4 AS
	  SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Heap5 AS
	  SELECT * FROM Base_Table_Heap1;

  CREATE OR REPLACE TABLE Base_Table_PK AS
    SELECT * FROM Base_Table_Heap1;
    ALTER TABLE Base_Table_PK ADD PRIMARY KEY (SaleKey) NOT ENFORCED;

  CREATE OR REPLACE TABLE Base_Table_Expiring
    OPTIONS (expiration_timestamp = cast(date_add(current_date(), interval 5 day) as timestamp)) AS
    SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Partitioned_Expiration
    PARTITION BY TIMESTAMP_TRUNC(InvoiceDateKey, DAY) 
    OPTIONS (partition_expiration_days = 5) AS
    SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Partitioned_TS
    PARTITION BY TIMESTAMP_TRUNC(InvoiceDateKey, DAY) AS
    SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Partitioned_DT
		PARTITION BY InvoiceDateKey AS
		SELECT
				SaleKey,
				CityKey,
				CustomerKey,
				BillToCustomerKey,
				StockItemKey,
				CAST(InvoiceDateKey AS DATE) AS InvoiceDateKey,
				DeliveryDateKey,
				SalespersonKey,
				WWIInvoiceID,
				Description,
				Package,
				Quantity,
				UnitPrice,
				TaxRate,
				TotalExcludingTax,
				TaxAmount,
				Profit,
				TotalIncludingTax,
				TotalDryItems,
				TotalChillerItems,
				LineageKey
		FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Partitioned_Req_Filter
    PARTITION BY TIMESTAMP_TRUNC(InvoiceDateKey, MONTH)
    OPTIONS (
      require_partition_filter = TRUE
    ) AS
    SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Partitioned_Range
    PARTITION BY RANGE_BUCKET(CustomerKey, GENERATE_ARRAY(0, 500, 25)) AS
    SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE TABLE Base_Table_Time_Ingestion
	(
		SaleKey INT64 NOT NULL,
		CityKey INT64 NOT NULL,
		CustomerKey INT64 NOT NULL,
		BillToCustomerKey INT64 NOT NULL,
		StockItemKey INT64 NOT NULL,
		InvoiceDateKey TIMESTAMP NOT NULL,
		DeliveryDateKey TIMESTAMP,
		SalespersonKey INT64 NOT NULL,
		WWIInvoiceID INT64 NOT NULL,
		Description STRING NOT NULL,
		Package STRING NOT NULL,
		Quantity INT64 NOT NULL,
		UnitPrice NUMERIC NOT NULL,
		TaxRate NUMERIC NOT NULL,
		TotalExcludingTax NUMERIC NOT NULL,
		TaxAmount NUMERIC NOT NULL,
		Profit NUMERIC NOT NULL,
		TotalIncludingTax NUMERIC NOT NULL,
		TotalDryItems INT64 NOT NULL,
		TotalChillerItems INT64 NOT NULL,
		LineageKey INT64 NOT NULL
	)
	PARTITION BY DATE(_PARTITIONTIME);

	CREATE OR REPLACE TABLE Complex_Types_Struct
	 (
	   SaleKey INT64,
	   CityKey INT64,
	   CustomerKey INT64,
	   InvoiceDateKey TIMESTAMP,
	   SalesItem STRUCT<
		 SaleItemKey INT64,
		 Quantity INT64,
		 UnitPrice NUMERIC
	   >
	 );

	CREATE OR REPLACE TABLE Complex_Types_Array
	 (
	   CustomerKey INT64,
	   CityKey INT64,
	   Meta STRUCT<
		SalespersonKey INT64
	   >,
	   Orders ARRAY<
		STRUCT<
		  SaleKey INT64,
		  InvoiceDateKey TIMESTAMP,
		  Quantity INT64,
		  UnitPrice NUMERIC
		>
	   >
	 );

	CREATE OR REPLACE VIEW Base_View_1 AS
	  SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE VIEW Base_View_2 AS
	  SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE VIEW Base_View_3 AS
	  SELECT * FROM Base_Table_Heap1;

	CREATE OR REPLACE VIEW Base_View_Schema_Evolution AS
	  SELECT * FROM Base_Table_Heap1;

	DROP MATERIALIZED VIEW IF EXISTS Base_Materialized_View_1;
	DROP MATERIALIZED VIEW IF EXISTS Base_Materialized_View_2;
	DROP MATERIALIZED VIEW IF EXISTS Base_Materialized_View_3;

	SET @@dataset_id = alt_integration_test_schema;

	CREATE OR REPLACE TABLE Alt_Base_Table_Heap1
	(
	  SaleKey INT64,
	  CityKey INT64,
	  CustomerKey INT64,
	  BillToCustomerKey INT64,
	  StockItemKey INT64,
	  InvoiceDateKey TIMESTAMP,
	  DeliveryDateKey TIMESTAMP,
	  SalespersonKey INT64,
	  WWIInvoiceID INT64,
	  Description STRING,
	  Package STRING,
	  Quantity INT64,
	  UnitPrice NUMERIC,
	  TaxRate NUMERIC,
	  TotalExcludingTax NUMERIC,
	  TaxAmount NUMERIC,
	  Profit NUMERIC,
	  TotalIncludingTax NUMERIC,
	  TotalDryItems INT64,
	  TotalChillerItems INT64,
	  LineageKey INT64
	);
END