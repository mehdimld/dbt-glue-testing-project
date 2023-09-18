# dbt-glue-testing

### pre-requisites : 
- Create an s3 bucket and replace it in your dbt-profile (location)
- Choose your table format if you want to use one. You can either use Parquet (default one), Iceberg, Hudi or Delta. If you want to use Iceberg, Hudi or Delta, just modify the datalake_formats field in your config by specifying iceberg, hudi or delta. Further configuration can be required : see this link [for Iceberg, this link for Delta, this link for Hudi](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-datalake-native-frameworks.html). For simplicity purpose, we have provided 4 profiles files in the profiles.yaml file in the config directory of this repo, one for each table format. If you wan to select one, just change the profile key value in the dbt-project.yaml file.  
- Create the Lake Formation tags if you want to test this feature
- Create an inline IAM policy with the following template 
- Create an IAM role named `AWSGlueServiceRole-dbt` that is the one mentionned in the profiles. 
- Attach the policy you've created to this role.

```yaml
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadAndWriteDatabases",
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
                "lakeformation:GetLFTag"
            ],
            "Resource": [
                "arn:aws:glue:eu-west-1:919960130949:catalog",
                "arn:aws:glue:eu-west-1:919960130949:table/jaffle_shop_schema/*",
                "arn:aws:glue:eu-west-1:919960130949:database/jaffle_shop_schema"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "ReadOnlyDatabases",
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
                "arn:aws:glue:eu-west-1:919960130949:table/jaffle_shop_schema/*",
                "arn:aws:glue:eu-west-1:919960130949:database/jaffle_shop_schema",
                "arn:aws:glue:eu-west-1:919960130949:database/default",
                "arn:aws:glue:eu-west-1:919960130949:database/global_temp"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "StorageAllBuckets",
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<YOUR_S3_BUCKET>",
                "arn:aws:s3:::<YOUR_S3_BUCKET>/*"
            ],
            "Effect": "Allow"
        },
        {
            "Sid": "ReadAndWriteBuckets",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<YOUR_S3_BUCKET>",
                "arn:aws:s3:::<YOUR_S3_BUCKET>/*"
            ],
            "Effect": "Allow"
        }
    ]
}
```
### Getting started 

```
python3 venv .venv
source .venv/bin/activate
pip install dbt-glue
dbt seed --profiles-dir ./config/
dbt run --select staging --profiles-dir ./config/
```
- Depending on the file format you choose, you can run either test classic_materization (ie. parquet), test_delta, test_hudi, test_iceberg (and modify your profile name in dbt-project.yml in consequence). You then can run the following commands : 
`dbt run --select test_<YOUR_TABLE_FORMAT>`
- If you want to run the test_lf_tags model, you'll need to create Lake Formation tag in your AWS Account : 
  - Login into the AWS Console with a principal that is Datalake Admin. 
  - Go to Permissions > LF-Tags and permissions
  - Click on 'Add LF-Tag' and complete the form with `sensitive` as key and `yes` and `no` as values.
  - Now you're able to run `dbt --run --select test_lf_tags`

