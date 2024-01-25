## SPARK1 Load File

```
dbutils.fs.ls("/FileStore/tables/data/retail-data/by-day")
```

> ls 
```
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("/FileStore/tables/data/retail-data/by-day/2010_12_01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")
```
>加载文件
![输入图片说明](/imgs/2024-01-24/thjQrvMbhWD7pA1K.png)
