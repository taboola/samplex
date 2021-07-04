package com.taboola.samplex;


import com.taboola.schemafilter.SchemaFilter;

import avro.shaded.org.apache.parquet.hadoop.metadata.CompressionCodecName;

public interface SamplexJob {
    default CompressionCodecName getParquetCompressionCodecName() {
        return CompressionCodecName.SNAPPY;
    }
    SamplexFilter getRecordFilter();
    String getDestinationFolder();
    default SchemaFilter getSchemaFilter(){
        return null;
    }
}
