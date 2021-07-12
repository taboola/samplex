Samplex
=====
Samplex is a Java library tool that allows, with help of Spark, 
to read Parquet data once and write multiple outputs by predefined filters

Read Once - Write Many!

Samplex was developed by [Taboola R&D Infrastructure Engineering](https://discover.taboola.com/taboola-infrastructure-engineering/) as part of the effort 
to optimize computation resources and Scale Up!

## Why It's needed   
When working with big raw data, we often seek to optimize downstream jobs by creating subsets of the raw data that can be repeatedly used by different jobs and can also be kept with higher retention than full raw data. 

The naive and straightforward approach would be to produce different subsets of the data, by reading the source data frame and then save the data multiple times with multiple predefined filters, as in the following example

```java
// Assuming you have SparkSession as `spark` variable
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

Dataset<Row> df = spark.read().parquet("fs://path/to/your/huge/parquet/data");
df.filter(col("age").gt(21)).write().parquet("fs://path/to/your/sub-set1");
df.filter("salary < 10000").write().parquet("fs://path/to/your/sub-set2");
...
```
In Spark, every write action would trigger the entire DAG for that action, 
so it will require Spark to scan the whole data several times as a number of write actions.

Caching the data may help when the input data is small compared to executor memory and disk resources. In our case, we are talking about inputs of Terabytes 

In addition, each subset produced, might benefit from predicate push-downs and other storage optimizations to avoid reading the entire data, but with many filters, and over very complex schema, we can’t store the full data optimized for every read path, without keeping different projections of the full data.


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

SamplexExecutor is using a low-level Spark `javaSparkContext.parallelize` method which distributes input files metadata and SamplexJob to executors.

Each executor reading each row of the Parquet file with `AvroParquetReader` applies filters defined in `SamplexJob` and writing relevant rows to output paths with `AvroParquetWriter`

We needed to use org.apache.parquet:parquet-avro:1.11.1 version to solve following [bug](https://issues.apache.org/jira/browse/PARQUET-1441).
Unfortunately, this version is not used in the latest Spark versions, yet, so we shaded it. 

## Configure

- You should have your output `FileSystem` configuration installed on your Spark executors 
- In order to fully utilise Samplex potential it is better to have (number of input files) >= `spark.default.parallelism`
- In case of load average is too high on executors, increase the number of CPUs per task `spark.task.cpus` 
