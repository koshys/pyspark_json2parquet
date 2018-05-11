The following works on local

source_path  would look under `/tmp/<source_path>/YYYY/MM/DD/HH24/`

**example commands :**

```bash
spark-submit pyspark_json2parquet/main.py --source "/my/data/" --env "qa" --date "2018-04-06 16"`
```

```bash
spark-submit pyspark_json2parquet/main.py --source "s3://mybucket/data/" --env "qa" --date "2018-01-01 01"
```
