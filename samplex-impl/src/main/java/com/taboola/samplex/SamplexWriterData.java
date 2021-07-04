package com.taboola.samplex;

import java.util.concurrent.BlockingQueue;

import avro.shaded.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SamplexWriterData {
    String jobId;
    String recordSchema;
    String outputFile;
    boolean useFieldNameModel;
    SamplexFilter samplexFilter;
    CompressionCodecName codecName;
    BlockingQueue<SamplexRecord> blockingQueue;
}
