package com.taboola.samplex;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import avro.shaded.org.apache.parquet.avro.AvroParquetWriter;
import avro.shaded.org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import avro.shaded.org.apache.parquet.hadoop.ParquetWriter;


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

        SamplexRecord samplexRecord;
        try (ParquetWriter<Object> parquetWriter = createAvroWriter(samplexWriterData)) {
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

    private ParquetWriter<Object> createAvroWriter(SamplexWriterData samplexWriterData) throws IOException {
        HdfsConfiguration conf = new HdfsConfiguration();
        Schema schema = new Schema.Parser().parse(samplexWriterData.getRecordSchema());
        Path outputFile = new Path(samplexWriterData.getOutputFile());
        AvroParquetWriter.Builder<Object> builder = AvroParquetWriter.builder(outputFile)
                .withConf(conf)
                .withCompressionCodec(samplexWriterData.getCodecName())
                .withWriteMode(Mode.OVERWRITE)
                .withSchema(schema);

        if (samplexWriterData.isUseFieldNameModel()){
            builder.withDataModel(GenericModelGetFieldByName.INSTANCE());
        }

        return builder.build();
    }
}
