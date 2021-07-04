package com.taboola.samplex;

import org.apache.avro.generic.GenericData.Record;

import lombok.Value;

@Value
public class SamplexRecord {
    boolean isLastRecord;
    Record record;
}
