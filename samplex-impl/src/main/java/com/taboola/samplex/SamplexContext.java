package com.taboola.samplex;

import java.io.Serializable;
import java.util.List;

import lombok.Value;

@Value
public class SamplexContext implements Serializable {

    String inputFile;
    String avroSchema;
    List<SamplexJobSpecificContext> samplexJobSpecificContextList;

    SamplexContext(String inputFile, List<SamplexJobSpecificContext> samplexJobSpecificContextList, String avroSchema) {
        this.inputFile = inputFile;
        this.samplexJobSpecificContextList = samplexJobSpecificContextList;
        this.avroSchema = avroSchema;
    }
}
