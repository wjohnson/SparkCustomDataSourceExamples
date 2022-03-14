# Creating Spark Data Sources with the Java API

## Quickstart

```bash
gradle clean build

spark-shell \
--jars=./lib/build/libs/dataguidebook-1.0-SNAPSHOT.jar
```

## Data Sources Version 1 API

This is the original way of defining a data source. It is still available in Spark 3.2.1.


```scala
val df = spark.read.format("com.dataguidebook.spark.datasource.v1").load("")

df.printSchema()
df.count()
```

### Working with Data Sources V1

[JavaDoc for Spark SQL Sources](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/sources/package-summary.html) provides you with all the classes you can use in your custom data source to define the behaviors. Need to provide two classes:
* DefaultSource which implements RelationProvider
* YourCustomClass which extends BaseRelation and implements at least TableScan

**Used for Reading or Writing**
* **RelationProvider**: Used in the **DefaultSource** and defines how you initialize a custom data source *without* a user defined schema.
  * Defines `createRelation` and is used when you do `spark.read` and takes in the options provided.
* **SchemaRelationProvider**: Used in the **DefaultSource** and defines how you initialize a custom data source *with* a user defined schema.
  * Defines `createRelation` and is used when you do `spark.read` and requires that you provide a schema.
* **DataSourceRegister**: Data sources should implement this trait so that they can register an alias to their data source.

**Reading Data**
* **TableScan**: A BaseRelation that can produce all of its tuples as an RDD of Row objects.
* **PrunedScan**: A BaseRelation that can eliminate unneeded columns before producing an RDD containing all of its tuples as Row objects.
* **PrunedFilteredScan**: A BaseRelation that can eliminate unneeded columns and filter using selected predicates before producing an RDD containing all matching tuples as Row objects.

**Writing Data**
* **CreatetableRelationProvider**: Creates a relation based on a dataframe.
* **InsertableRelation**: A BaseRelation that can be used to insert data into it through the insert method.

## Spark 2.4 Data Sources V2

In [Spark 2.3, the Data Sources V2 API (JavaDoc)](https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/sources/v2/package-summary.html) was released in beta ([Spark JIRA](https://issues.apache.org/jira/browse/SPARK-15689)) but was not marked as stable until 2.4. So, we'll only talk about the [Spark 2.4.7 Data Sources V2 API (JavaDoc)](https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/sql/sources/v2/package-summary.html)

**Used for Reading or Writing**
* **DataSourceV2** is a "marker interface" which essentially tags the class but doesn't define any behavior.
* (org.apache.spark.sql.catalyst)**InternalRow** is a binary format used inside of Apache Spark.
  * TODO: How do you make an internal row

**Reading Data**
* **ReadSupport**: Requires you implement the `createReader` method to return a `DataSourceReader` object.
* **DataSourceReader**: Requires you implement `readSchema` (when no schema is provided) and `planInputPartitions` which returns the set of partitions being used. Each partition would create its own data source reader to handle that partition of data.
* **InputPartition**:
* **InputPartitionReader**:



**Writing Data**


## Spark 3 Data Sources V2

In Spark 3, the Data Sources V2 API was revised AGAIN and should really be called the V3 API.

**Used for Reading or Writing**

**Reading Data**


**Writing Data**


## Other References

These blogs, videos, and repos have been extremely helpful in improving my understanding of the history of the Data Source API in Apache Spark.

### Spark 3 DataSources V2 References
* [blog.madhukaraphatak.com Data Sources V2 (Spark 3.0)](http://blog.madhukaraphatak.com/categories/datasource-v2-spark-three/)

### Spark 2 DataSources V2 References
* [Spark 2.4.7 JavaDoc for Data Sources V2 API](https://spark.apache.org/docs/2.4.7/api/java/org/apache/spark/sql/sources/v2/package-summary.html)
* [Spark 2.3.0 JavaDoc for Data Sources V2 API](https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/sources/v2/package-summary.html)
* [(2018 Spark Summit) Data Source V2 (Spark 2.3)](https://www.youtube.com/watch?v=9-eomYXVnvY) starts at 9:19
  * Includes a review of Data Sources V1
* [blog.madhukaraphatak.com Data Sources V2 (Spark 2.3)](http://blog.madhukaraphatak.com/categories/datasource-v2-series/)
* [shzhangji.com Data Source V2 (Spark 2.3)](http://shzhangji.com/blog/2018/12/08/spark-datasource-api-v2/)

### Spark DataSources V1 References
* [Spark Latest JavaDoc for Spark SQL Sources](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/sources/package-summary.html)
* 2019 Spark Summit: Jacek Laskowski Live Coding Session (Spark 2.4):
  * The title says it's about Spark 2.4 but he never actually talks about 2.4 interfaces
  * [Part 1](https://www.youtube.com/watch?v=YKkgVEgn2JE)
  * [Part 2](https://www.youtube.com/watch?v=vfd83ELlMfc)
* [Spark in Action Book Ch 9 Data Source V1](https://www.manning.com/books/spark-in-action-second-edition)
  * [Github repo with complex example of V1](https://github.com/jgperrin/net.jgp.books.spark.ch09/tree/master)
  * [(2017 Spark Summit) ](https://www.youtube.com/watch?v=M6NdFsKJ7os)
* [(2016 Spark Summit) Data Sources V1](https://www.youtube.com/watch?v=O9kpduk5D48)