Samplex
=====
Samplex is a Java library tool which allows, with help of Spark, 
to read Parquet data once and write multiple outputs by predefined filters

Read Once - Write Many!

Samplex was developed by [Taboola Infrastructure Engineering Team](https://discover.taboola.com/taboola-infrastructure-engineering/) as part of the effort 
to optimize resource usages for saving sub-sets of huge data by different teams.

## Why its needed   
When working with extremely big inputs of raw data, it is often needed to create
sub-set of it in order to keep work more efficient in downstream or to keep the relevant sub-set with longer retention.

Usually, when working with Parquet data in Spark 
when you want to create different sub-sets of your data you will do following:

```java
// Assuming you have SparkSession as `spark` variable
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

Dataset<Row> df = spark.read().parquet("fs://path/to/your/huge/parquet/data");
df.filter(col("age").gt(21)).write().parquet("fs://path/to/your/sub-set1");
df.filter("salary < 10000").write().parquet("fs://path/to/your/sub-set2");
...
```
Usually such operations will require from Spark to scan the data number of times as number of save actions.
Yes, when you have your input dataframe saved in ordered or/and bucketed format it may perform faster,
but you can't predict by which filter users of the data will filter this data tomorrow.  

Additionally, it is possible to cache the data to Spark storage to perform further operations quicker,
but let's not forget we are dealing with huge amounts of data and this will be a costly operation by itself.

## Usage

```java
// Assuming you have SparkSession as `spark` variable
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.taboola.samplex.SamplexExecutor;

SamplexExecutor samplexExecutor = new SamplexExecutor(spark);

// File system to save in, may be local aka file:/// or HDFS aka hdfs://your_name_node
final FileSystem fileSystem = FileSystem.get(new Configuration());

// Define one or more SamplexJobs
SamplexJob filterByAge = new SamplexJob() {
    // Filter is based on Avro record which represents one row of your data
    @Override
    public SamplexFilter getRecordFilter() {
            return (record) -> "GB".equals(record.get("diedCountryCode"));
    }
    
    @Override
    public String getDestinationFolder() {
            return "/output/path/to/your/sub-set/";
    }
};

// Define number of output files, bounded by number of input files in DF from the bottom
int numberOfOutputFiles = 1;
samplexExecutor.executeSamplex(
    fileSystem,
    nobelPrizeWinners,
    Arrays.asList(filterByAge),
    numberOfOutputFiles);
```
## How it works

SamplexExecutor is using low level Spark `javaSparkContext.parallelize` method which distribute
input files metadata and SamplexJob to executors.

Each executor reading each row of the Parquet file with `AvroParquetReader` applies
filters defined in Samplex and writing relevant rows to output paths with `AvroParquetWriter`

## Configure

- You should have your output `FileSystem` configuration installed on your Spark executors 
- In order to fully utilise Samplex potential 
  it is better to have (number of input files) >= `spark.default.parallelism`
- In case load average is too high on executors, increase number of cpus per task `spark.task.cpus` 
