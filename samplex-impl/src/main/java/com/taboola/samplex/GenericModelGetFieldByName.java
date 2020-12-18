package com.taboola.samplex;

import org.apache.avro.generic.GenericData;

public class GenericModelGetFieldByName extends GenericData {
    public static GenericModelGetFieldByName instance = new GenericModelGetFieldByName();

    private GenericModelGetFieldByName(){
        super();
    }

    public static GenericModelGetFieldByName INSTANCE(){
        return instance;
    }

    @Override
    public Object getField(Object record, String name, int position) {
        return ((Record)record).get(name);
    }
}
