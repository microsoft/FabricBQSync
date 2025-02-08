# Fabric Sync Column Mapping

The pillar of the Fabric Sync accelerator is to act as a friction-free mirror of BigQuery data and not be another ETL/ELT tool. In support of this, the framework attempts to minimize data transformation to only what is absolutely required for the data to be useful and performant within Fabric.

While following these guidelines, its common to support the following scenarios between the source and mirrored destination:
- Rename a colum
- Change a column data type
- Drop a column

While these changes could (and arguable should) be made at the source, Column Mapping is a table-level configuration that provides a lightweight alternative approach when the prior is not possible.

#### Supported Data Type Conversions
In keeping with the limited transformation principles a small set of data type transformations are supported.

The support data type conversions are:
- STRING_TO_DATE - Uses to_date which requires a format and throws exceptions on invalid input
- STRING_TO_TIMESTAMP - Uses to_timestamp which requires a format and throws exceptions on invalid input
- STRING_TO_INT - Uses cast and throws exceptions on invalid input
- STRING_TO_LONG - Uses cast and throws exceptions on invalid input
- STRING_TO_DECIMAL - Uses try_to_number and returns None on invalid conversion
- DATE_TO_STRING - Uses date_format which requires a format
- TIMESTAMP_TO_STRING - Uses date_format which requires a format
- INT_TO_STRING - Uses cast
- DECIMAL_TO_STRING - Uses cast
- LONG_TO_STRING - Uses cast

## Column Mapping Examples

### Rename a Column
In the example below, the SalesPersonKey column of the SalesOrder table is renamed to EmployeeNumber.

<code>
{
    ...
    "tables": [
        {
            "table_name": "SalesOrder",
            "column_map": [
                {
                    "source": {
                        "name":"SalesPersonKey"
                    },
                    "destination": {
                        "name":"EmployeeNumber"
                    }
                }
            ]
        }
    ]
}
</code>

### Change a Column Data Type
In the example below, the DeliveryDate column of the SalesOrder table is converted from a STRING to a TIMESTAMP datatype. Note that the format pattern is supplied following [PySpark DateTime Format](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html).

<code>
{
    ...
    "tables": [
		{
            "table_name": "SalesOrder",
            "column_map": [
                {
                    "source": {
                        "name":"DeliveryDate",
                        "type":"STRING"
                    },
                    "destination": {
                        "name":"DeliveryDate",
                        "type":"TIMESTAMP"
                    },
                    "format": "yyyy-MM-dd HH:mm"
                }
            ]
        }
    ]
}
</code>

### Drop a Column
In this example, the AltRouteNumber column of the SalesOrder table is dropped during synchronization and not included in the mirrored data.

<code>
{
    ...
    "tables": [
        {
            "table_name": "SalesOrder",
            "column_map": [
                {
                    "source": {
                        "name":"AltRouteNumber"
                    },
                    "drop_source": true
                }
            ]
        }
    ]
}
</code>
