package com.taboola.samplex;

import java.io.Serializable;

import com.taboola.schemafilter.SchemaFilter;

import avro.shaded.org.apache.parquet.hadoop.metadata.CompressionCodecName;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class SamplexJobSpecificContext implements Serializable {
    String outputPath;
    String jobId;
    CompressionCodecName codecName;
    SamplexFilter samplexFilter;
    SchemaFilter schemaFilter;
}
