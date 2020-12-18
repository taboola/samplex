package com.taboola.samplex;

import java.io.Serializable;

import com.taboola.schemafilter.SchemaFilter;

import lombok.Builder;
import lombok.Value;

@Value
public class SamplexJobSpecificContext implements Serializable {

    String outputPath;
    String jobId;
    SamplexFilter samplexFilter;
    SchemaFilter schemaFilter;

    @Builder
    public SamplexJobSpecificContext(String outputPath, String jobId, SamplexFilter samplexFilter, SchemaFilter schemaFilter) {
        this.outputPath = outputPath;
        this.jobId = jobId;
        this.samplexFilter = samplexFilter;
        this.schemaFilter = schemaFilter;
    }
}
