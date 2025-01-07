DECLARE integration_test_schema STRING DEFAULT 'IntegrationTest';
--SET @@dataset_project_id = '';

BEGIN
  SET @@dataset_id = integration_test_schema;
  
  CREATE TEMP TABLE test_data AS
  SELECT *
  FROM SourceData  
  WHERE InvoiceDateKey = (
    SELECT DATE_ADD(MAX(InvoiceDateKey), interval 1 day)
    FROM Base_Table_Heap1
  );

  INSERT INTO Base_Table_PK
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Heap1
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Partitioned_Expiration
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Partitioned_TS
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Partitioned_DT
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
  FROM test_data;

  INSERT INTO Base_Table_Partitioned_Req_Filter
  SELECT * FROM test_data;

  INSERT Base_Table_Time_Ingestion
  (
      SaleKey,
      CityKey,
      CustomerKey,
      BillToCustomerKey,
      StockItemKey,
      InvoiceDateKey,
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
  )
  SELECT * FROM test_data;

  DROP VIEW IF EXISTS Base_View_Schema_Evolution;
  CREATE VIEW Base_View_Schema_Evolution AS
    SELECT SaleKey AS SaleAltKey, * FROM Base_Table_Heap1;

  CALL BQ.REFRESH_MATERIALIZED_VIEW('Base_Materialized_View_1');
  CALL BQ.REFRESH_MATERIALIZED_VIEW('Base_Materialized_View_2');
  CALL BQ.REFRESH_MATERIALIZED_VIEW('Base_Materialized_View_3');
END