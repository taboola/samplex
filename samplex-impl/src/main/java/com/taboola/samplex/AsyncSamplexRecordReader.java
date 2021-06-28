package com.taboola.samplex;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import avro.shaded.org.apache.parquet.avro.AvroParquetReader;
import avro.shaded.org.apache.parquet.avro.AvroReadSupport;
import avro.shaded.org.apache.parquet.avro.AvroSchemaConverter;
import avro.shaded.org.apache.parquet.hadoop.ParquetReader;


public class AsyncSamplexRecordReader implements Runnable {

    private static final Logger logger = LogManager.getLogger(AsyncSamplexRecordReader.class);
    private static final long MAX_WAIT_QUEUE_OFFER_SECONDS = 10L;

    private final String inputFile;
    private final List<SamplexWriterData> samplexWriterDataList;
    private final int taskId;

    AsyncSamplexRecordReader(String inputFile, List<SamplexWriterData> samplexWriterDataList, int taskId) {
        this.inputFile = inputFile;
        this.samplexWriterDataList = samplexWriterDataList;
        this.taskId = taskId;
    }

    @Override
    public void run() {
        String defaultName = Thread.currentThread().getName();
        String fileName = inputFile.substring(StringUtils.lastOrdinalIndexOf(inputFile, "/", 1));
        logger.info("Going to read file [" + fileName + "], task id [" + taskId + "]");
        Thread.currentThread().setName(defaultName + "-" + fileName);

        HdfsConfiguration conf = new HdfsConfiguration();
        conf.set(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, "false");
        try (ParquetReader<Record> avroParquetReader = AvroParquetReader.builder(new AvroReadSupport<Record>(), new Path(inputFile))
                .withConf(conf)
                .build()) {

            Record pageViewRecord = avroParquetReader.read();
            if (pageViewRecord == null) {
                logger.info("Got empty file [" + inputFile + "], skipping");
                return;
            }

            while (pageViewRecord != null) {
                Record finalPageViewRecord = pageViewRecord;
                samplexWriterDataList.stream()
                        .filter(samplexWriterData -> samplexWriterData.getSamplexFilter().isFilterMatching(finalPageViewRecord))
                        .forEach(samplexWriterData -> {
                            boolean wasAdded;
                            try {
                                wasAdded = samplexWriterData.getBlockingQueue().offer(new SamplexRecord(false, finalPageViewRecord), MAX_WAIT_QUEUE_OFFER_SECONDS, TimeUnit.SECONDS);
                                if (!wasAdded) {
                                    throw new RuntimeException("Failed insert to queue [" + samplexWriterData.getBlockingQueue().hashCode() + "] of job [" + samplexWriterData.getJobId() + "], task id [" + taskId + "], exit");
                                }
                            } catch (InterruptedException e) {
                                logger.error("Interrupted while offer on queue, job [" + samplexWriterData.getJobId() + "], task id [" + taskId + "], there might be an issue with writer, check log", e);
                                throw new RuntimeException(e);
                            }
                        });

                pageViewRecord = avroParquetReader.read();
            }
            logger.info("Finished to read file [" + fileName + "], task id [" + taskId + "]");
        } catch (Throwable t) {
            logger.error("Failed to read input file [" + inputFile + "], task id [" + taskId + "]", t);
            throw new RuntimeException(t);
        } finally {
            Thread.currentThread().setName(defaultName);
        }
    }
}
