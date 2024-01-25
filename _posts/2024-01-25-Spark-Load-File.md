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

![输入图片说明](blob:https://stackedit.cn/c2786e5a-00fc-4b0c-adb9-61f9af1b1afb)
