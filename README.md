# dbt-glue-testing

### pre-requisites : 

- use the following template as dbt profile. And fill it with your own bucket, roles, ..
```
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: glue
      query-comment: my comment
      role_arn: arn:aws:iam::<your_account_id>:role/AWSGlueServiceRole-dbt
      region: <your_aws_region>
      workers: 2
      worker_type: G.1X
      schema: "<your_schema_name>"
      session_provisioning_timeout_in_seconds: 120
      location: "s3://<your_s3_bucket_uri>"
      datalake_formats: iceberg #Could be hudi or delta
      conf: spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog  --conf spark.sql.catalog.glue_catalog.warehouse=s3://<your_s3_bucket_uri> --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions 
```
- create an s3 bucket named <demo-dbt-output-YOUR_AWS_ACCOUNT_ID> and replace it in your dbt-profile (location and configuration >> --conf spark.sql.warehouse.dir=<bucket-name>/<schema> fields)
- use the following policy and attach it to a role named `AWSGlueServiceRole-dbt` that is the one mentionned in the profiles. 

```yaml
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Read_and_write_databases",
            "Action": [
                "glue:SearchTables",
                "glue:BatchCreatePartition",
                "glue:CreatePartitionIndex",
                "glue:DeleteDatabase",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:DeleteTableVersion",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "glue:DeletePartitionIndex",
                "glue:GetTableVersion",
                "glue:UpdateColumnStatisticsForTable",
                "glue:CreatePartition",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "glue:UpdateColumnStatisticsForPartition",
                "glue:CreateDatabase",
                "glue:BatchDeleteTableVersion",
                "glue:BatchDeleteTable",
                "glue:DeletePartition",
                "glue:GetUserDefinedFunctions",
                "lakeformation:ListResources",
                "lakeformation:BatchGrantPermissions",
                "lakeformation:ListPermissions", 
                "lakeformation:GetDataAccess",
                "lakeformation:GrantPermissions",
                "lakeformation:RevokePermissions",
                "lakeformation:BatchRevokePermissions",
                "lakeformation:AddLFTagsToResource",
                "lakeformation:RemoveLFTagsFromResource",
                "lakeformation:GetResourceLFTags",
                "lakeformation:ListLFTags",
                "lakeformation:GetLFTag",
            ],
            "Resource": [
                "arn:aws:glue:<region>:<AWS Account>:catalog",
                "arn:aws:glue:<region>:<AWS Account>:table/<dbt output database>/*",
                "arn:aws:glue:<region>:<AWS Account>:database/<dbt output database>"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Read_only_databases",
            "Action": [
                "glue:SearchTables",
                "glue:GetTableVersions",
                "glue:GetPartitions",
                "glue:GetTableVersion",
                "glue:GetTables",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "lakeformation:ListResources",
                "lakeformation:ListPermissions"
            ],
            "Resource": [
                "arn:aws:glue:<region>:<AWS Account>:table/<dbt source database>/*",
                "arn:aws:glue:<region>:<AWS Account>:database/<dbt source database>",
                "arn:aws:glue:<region>:<AWS Account>:database/default",
                "arn:aws:glue:<region>:<AWS Account>:database/global_temp"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Storage_all_buckets",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<dbt output bucket>",
                "arn:aws:s3:::<dbt source bucket>"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Read_and_write_buckets",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<dbt output bucket>"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "Read_only_buckets",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::<dbt source bucket>"
            ],
            "Effect": "Allow"
        }
    ]
}
```
### Getting started 

```
python3 venv .venv
pip install dbt-glue
source .venv/bin/activate
dbt seed
dbt run --select staging
dbt run --select customers
``` 
