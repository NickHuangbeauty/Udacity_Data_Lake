# Udacity_Data_Lake

## Connecting seesion
```python
def create_spark_session():
    spark = SparkSession.builder \
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
                        .getOrCreate()
    return spark
```

## Song data
### It just using one function to load, create and insert data
```python
def 
```


## Log data
### It just using one function to load, create and insert data