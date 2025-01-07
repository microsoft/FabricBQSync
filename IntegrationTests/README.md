# Fabric Sync Integration Tests

### GCP Environment Set-Up
The Integration Test does not set-up or configure the GCP environment for the tests. Manual configuration is required to run the suite of Integration Tests.

Integration Tests uses two BigQuery datasets and one source data table. Use the following steps to configure your GCP environment.
1. Create a new dataset called <code>IntegrationTest</code>
2. In the new dataset, load the test data from the <code>Data/integration_test_data.snappy.parquet</code> to a native table named <code>SourceData</code>.
3. The following scripts are used:
    - <code>BQ_Scripts/BQ_Create_Tables.sql</code> - creates the required BigQuery objects.
    - <code>BQ_Scripts/BQ_Initial_Load.sql</code> - populates the initial dataset.
    - <code>BQ_Scripts/BQ_Incremental_Load.sql</code> - populates an incremental dataset

    Adjust the <code>dataset_project_id</code> and <code>integration_test_schema</code> if required in your environment.


### Running the Integration Test


### Integration Test Evaluation