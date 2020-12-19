package com.taboola.samplex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.rules.TemporaryFolder;

import com.databricks.spark.avro.SchemaConverters;
import com.taboola.schemafilter.SchemaFilter;

public class SamplexTestUtil {

    private final SamplexMultiplexWriter samplexMultiplexWriter = new SamplexMultiplexWriter();

    private final TemporaryFolder folder;

    public SamplexTestUtil(TemporaryFolder folder) {
        this.folder = folder;
    }

    public Dataset<Row> getOutputDataFrameByFilter(Dataset<Row> inputDf, SamplexFilter samplexFilter) throws Exception {
        return getOutputDataFrameByFilter(inputDf, samplexFilter, null);
    }

    public Dataset<Row> getOutputDataFrameByFilter(Dataset<Row> inputDf, SamplexFilter samplexFilter, SchemaFilter schemaFilter) throws Exception {

        SparkSession sparkSession = SparkForTest.sparkSession;
        String samplexInputFolder = folder.newFolder("input").getPath();
        String samplexOutputFolder = folder.newFolder("output").getPath();
        inputDf.coalesce(1).write().mode(SaveMode.Overwrite).parquet(samplexInputFolder);

        Dataset<Row> rowDataset = sparkSession.read().parquet(samplexInputFolder);
        String[] inputFiles = rowDataset.inputFiles();

        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("spark_schema").namespace(null);
        Schema dfAvroSchema = SchemaConverters.convertStructToAvro(rowDataset.schema(), recordBuilder, null);

        SamplexJobSpecificContext specificContext = new SamplexJobSpecificContext(samplexOutputFolder, "general_job", samplexFilter, schemaFilter);

        List<SamplexContext> samplexContexts = Arrays.stream(inputFiles).map(i -> new SamplexContext(i, Collections.singletonList(specificContext), dfAvroSchema.toString())).collect(Collectors.toList());
        samplexMultiplexWriter.call(samplexContexts.iterator());
        return sparkSession.read().parquet(samplexOutputFolder);
    }
}
