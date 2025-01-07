DECLARE integration_test_schema STRING DEFAULT 'IntegrationTest';
--SET @@dataset_project_id = '';

BEGIN
  DECLARE alt_integration_test_schema STRING DEFAULT CONCAT(integration_test_schema, '_2');
  SET @@dataset_id = integration_test_schema;

  CREATE TEMP TABLE test_data AS
    SELECT *
    FROM SourceData 
    WHERE InvoiceDateKey >= "2000-01-01"
    AND InvoiceDateKey < "2000-01-03";

  INSERT INTO Base_Table_Expiring
  SELECT * FROM test_data;

  INSERT INTO Base_Table_PK
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Heap1
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Heap2
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Heap3
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Heap4
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Partitioned_Expiration
  SELECT
      SaleKey,
      CityKey,
      CustomerKey,
      BillToCustomerKey,
      StockItemKey,
      CURRENT_TIMESTAMP() AS InvoiceDateKey,
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

  INSERT INTO Base_Table_Partitioned
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Partitioned_Req_Filter
  SELECT * FROM test_data;

  INSERT INTO Base_Table_Partitioned_Range
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

  INSERT INTO Complex_Types_Struct (SaleKey, CityKey, CustomerKey, InvoiceDateKey, SalesItem) 
  SELECT SaleKey, CityKey, CustomerKey, InvoiceDateKey, 
    STRUCT(SaleKey, Quantity, UnitPrice)
  FROM test_data;

  INSERT INTO Complex_Types_Array (CustomerKey, CityKey, Meta, Orders)
  SELECT
    CustomerKey, CityKey, 
    STRUCT(SalespersonKey),
    ARRAY_AGG(STRUCT(SaleKey, InvoiceDateKey, Quantity, UnitPrice))
  FROM test_data
  GROUP BY CustomerKey, CityKey, SalespersonKey;

  EXECUTE IMMEDIATE
    CONCAT('CREATE MATERIALIZED VIEW Base_Materialized_View_1 OPTIONS (enable_refresh = true) AS SELECT * FROM ', integration_test_schema, '.Base_Table_Heap1');

  EXECUTE IMMEDIATE
    CONCAT('CREATE MATERIALIZED VIEW Base_Materialized_View_2 OPTIONS (enable_refresh = true) AS SELECT * FROM ', integration_test_schema, '.Base_Table_Heap1');

  EXECUTE IMMEDIATE
    CONCAT('CREATE MATERIALIZED VIEW Base_Materialized_View_3 OPTIONS (enable_refresh = true) AS SELECT * FROM ', integration_test_schema, '.Base_Table_Heap1');

  SET @@dataset_id = alt_integration_test_schema;

  EXECUTE IMMEDIATE
    CONCAT('INSERT INTO Alt_Base_Table_Heap1 SELECT * FROM ', integration_test_schema, '.Base_Table_Heap1');
END