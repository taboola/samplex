package com.taboola.samplex.schemafilter;

import java.io.Serializable;

public interface SchemaFilter extends Serializable {
    String filter(String schema);
}
