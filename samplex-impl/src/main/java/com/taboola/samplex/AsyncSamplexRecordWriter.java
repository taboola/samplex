package com.taboola.samplex;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import avro.shaded.org.apache.parquet.avro.AvroParquetWriter;
import avro.shaded.org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import avro.shaded.org.apache.parquet.hadoop.ParquetWriter;
import avro.shaded.org.apache.parquet.hadoop.metadata.CompressionCodecName;


public class AsyncSamplexRecordWriter implements Runnable {

    private static final Logger logger = LogManager.getLogger(AsyncSamplexRecordWriter.class);

    private final SamplexWriterData samplexWriterData;
    private final int taskId;

    AsyncSamplexRecordWriter(SamplexWriterData samplexWriterData, int taskId) {
        this.samplexWriterData = samplexWriterData;
        this.taskId = taskId;
    }

    @Override
    public void run() {
        String defaultName = Thread.currentThread().getName();
        String jobId = samplexWriterData.getJobId();
        Thread.currentThread().setName(defaultName + "-" + jobId);
        HdfsConfiguration conf = new HdfsConfiguration();
        conf.set("parquet.avro.write-old-list-structure", "false");
        conf.set("fs.permissions.umask-mode", "000");
        SamplexRecord samplexRecord;

        Schema schema = new Schema.Parser().parse(samplexWriterData.getRecordSchema());
        try (ParquetWriter<Object> parquetWriter = createAvroWriter(conf, samplexWriterData.getOutputFile(), schema, samplexWriterData.isUseFieldNameModel())) {
            logger.info("Created writer [" + parquetWriter.hashCode() + "] for [" + jobId + "], queue [" + samplexWriterData.getBlockingQueue().hashCode() + "], task id [" + taskId + "]");
            while (!(samplexRecord = samplexWriterData.getBlockingQueue().take()).isLastRecord()) {
                parquetWriter.write(samplexRecord.getRecord());
            }
            logger.info("Finished to write data for [" + jobId + "], task id [" + taskId + "]");
        } catch (Throwable t) {
            logger.error("Failed to write data for [" + jobId + "], task id [" + taskId + "]", t);
            throw new RuntimeException(t);
        } finally {
            Thread.currentThread().setName(defaultName);
        }
    }

    private ParquetWriter<Object> createAvroWriter(Configuration conf, String outputPath, Schema schema, boolean useFieldNameModel) throws IOException {
        AvroParquetWriter.Builder<Object> builder = AvroParquetWriter.builder(new Path(outputPath))
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(Mode.OVERWRITE)
                .withSchema(schema);

        if (useFieldNameModel){
            builder.withDataModel(GenericModelGetFieldByName.INSTANCE());
        }

        return builder.build();
    }
}
