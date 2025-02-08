# Fabric Sync - Inital Loads/Historical Data

In most cases, initial loading of historical data by the Fabric Sync accelerator can be handled directly and efficiently without any special considerations.

In cases where there is a substantial volume of historical data or there is a requirement to shard the data movement the following approaches can be utilized.

### Table Configuration - Source Query

The Fabric Sync framework allows BigQuery objects to be reshaped through the definition of a <code>source_query</code> in the table configuration. The <code>source_query</code> can be any valid BigQuery SQL query and is used in-place for the named BigQuery object.

For historical loads, the <code>source_query</code> table configuration can be manually modified/updated to incrementally load slices of data as required.

<b><u>Example <code>source_query</code> Configuration</u></b>

<code>
{
    "table_name": "WebSalesTransactions",
    "dataset": "Sales",
    "object_type": "BASE_TABLE",
    "source_query": "SELECT * FROM myproj.sales.websalestransactions WHERE transaction_ts BETWEEN '2024-01-01 00:00:00' AND '2024-12-31 23:59:59'",
}

</code>

### Table Configuration - Predicate

An alternate approach, yet similar approach to the above uses the <code>predicate</code> table configuration. Instead of defining a full BigQuery SQL query, the <code>predicate</code> configuration is a SQL filter that is added before synching data from the target BigQuery object.

<b><u>Example <code>predicate</code> Configuration</u></b>

<code>
{
    "table_name": "WebSalesTransactions",
    "dataset": "Sales",
    "object_type": "BASE_TABLE",
    "predicate": "transaction_ts BETWEEN '2024-01-01 00:00:00' AND '2024-12-31 23:59:59'",
}

</code>
