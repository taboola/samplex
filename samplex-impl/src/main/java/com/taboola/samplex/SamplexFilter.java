package com.taboola.samplex;

import java.io.Serializable;

import org.apache.avro.generic.GenericData.Record;

public interface SamplexFilter extends Serializable {
    boolean isFilterMatching(Record record);
}
