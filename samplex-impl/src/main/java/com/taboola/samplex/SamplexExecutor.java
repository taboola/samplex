package com.taboola.samplex;

import static java.lang.Math.min;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.databricks.spark.avro.SchemaConverters;

public class SamplexExecutor {

    private final static Logger logger = LogManager.getLogger(SamplexExecutor.class);

    private final JavaSparkContext javaSparkContext;

    public SamplexExecutor(SparkSession sparkSession) {
        this.javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }

    public void executeSamplex(FileSystem outputFileSystem, Dataset<Row> inputDataset, List<SamplexJob> samplexJobs, int numberOutputFiles) throws IOException {

        List<SamplexJobSpecificContext> jobSpecificContexts = getSamplexJobSpecificContexts(samplexJobs);

        if (!jobSpecificContexts.isEmpty()) {
            logger.info("Going to execute following samplex jobs: \n" + jobSpecificContexts.stream()
                            .map(specificContext -> (specificContext.getJobId() + ", output_parh: " + specificContext.getOutputPath()))
                            .collect(Collectors.joining("\n")));

            List<SamplexContext> writeOutputLists = getWorkingFilesList(jobSpecificContexts, inputDataset);
            // Min need when num of read partitions is less then what is set in config
            JavaRDD<SamplexContext> javaRDD = javaSparkContext.parallelize(writeOutputLists, min(numberOutputFiles, writeOutputLists.size()));

            try {
                SamplexFileUtil.removeOutputFolders(jobSpecificContexts, outputFileSystem);
                javaRDD.foreachPartition(new SamplexMultiplexWriter());
                handleSuccess(outputFileSystem, jobSpecificContexts);
            } catch (Throwable t) {
                logger.info("Failed to execute Samplex jobs",t);
                handleFailure(outputFileSystem, jobSpecificContexts);
                throw t;
            }
        } else {
            logger.info("No active samplex jobs found ");
        }
    }

    private void handleSuccess(FileSystem fileSystem, List<SamplexJobSpecificContext> jobSpecificContexts) {
        SamplexFileUtil.removeFailedTasksFiles(jobSpecificContexts, fileSystem);
        SamplexFileUtil.writeSuccessFiles(jobSpecificContexts, fileSystem);
    }

    private List<SamplexContext> getWorkingFilesList(List<SamplexJobSpecificContext> jobSpecificContexts, Dataset<Row> inputDataFrame) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("spark_schema").namespace(null);
        Schema dfAvroSchema = SchemaConverters.convertStructToAvro(inputDataFrame.schema(), recordBuilder, null);

        return Arrays.stream(inputDataFrame.inputFiles())
                .map(inputFile -> new SamplexContext(inputFile, jobSpecificContexts, dfAvroSchema.toString()))
                .collect(Collectors.toList());
    }

    private void handleFailure(FileSystem fileSystem, List<SamplexJobSpecificContext> jobSpecificContexts) {
        try {
            SamplexFileUtil.removeOutputFolders(jobSpecificContexts, fileSystem);
        } catch (IOException e) {
            logger.error("failed to remove temporary folders", e);
        }
    }


    private List<SamplexJobSpecificContext> getSamplexJobSpecificContexts(List<SamplexJob> samplexJobs) {
        return samplexJobs.stream()
                    .map(samplexJob -> new SamplexJobSpecificContext(
                            samplexJob.getDestinationFolder(),
                            samplexJob.getClass().getSimpleName(),
                            samplexJob.getRecordFilter(),
                            samplexJob.getSchemaFilter()))
                    .collect(Collectors.toList());
    }
}
