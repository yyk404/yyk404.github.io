## SPARK1 Load File

```
dbutils.fs.ls("/FileStore/tables/data/retail-data/by-day")
```

> ls

```scala
val df = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.load("/FileStore/tables/data/retail-data/by-day/2010_12_01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")
```
>加载文件

{% include aligner.html images="blob:https://stackedit.cn/c2786e5a-00fc-4b0c-adb9-61f9af1b1afb" %}

