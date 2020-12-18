package com.taboola.schemafilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;


public class TopLevelFieldsSchemaBlacklistFilterTest {
    private static final String schema = "{\"type\":\"record\",\"name\":\"spark_schema\",\"fields\":[{\"name\":\"geoRegionId\",\"type\":[\"long\",\"null\"]},{\"name\":\"consentBit\",\"type\":[\"string\",\"null\"]},{\"name\":\"country\",\"type\":[\"string\",\"null\"]},{\"name\":\"countryCode\",\"type\":[\"string\",\"null\"]},{\"name\":\"isControlGroup\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"configLabel\",\"type\":[\"string\",\"null\"]},{\"name\":\"deviceId\",\"type\":[\"string\",\"null\"]}]}";
    private final JSONObject jsonSchema = new JSONObject(schema);

    @Test
    public void filter_filterSetIsEmpty_returnUntouched() throws JSONException {
        TopLevelFieldsSchemaBlacklistFilter filter = new TopLevelFieldsSchemaBlacklistFilter(new HashSet<>());
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertEquals(jsonSchema.toString(), filteredJson.toString());
    }

    @Test
    public void filter_schemaHasNoFieldsFromFilterSet_returnUntouched() throws JSONException {
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add("doesNotExist2");
        TopLevelFieldsSchemaBlacklistFilter filter = new TopLevelFieldsSchemaBlacklistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertEquals(jsonSchema.toString(), filteredJson.toString());
    }

    @Test
    public void filter_schemaHasFieldsFromFilterSet_returnFiltered() throws JSONException {
        String fieldToFilter = "geoRegionId";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter);
        TopLevelFieldsSchemaBlacklistFilter filter = new TopLevelFieldsSchemaBlacklistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertNotEquals(jsonSchema.toString(), filteredJson.toString());
        assertFalse(filteredSchema.contains(fieldToFilter));
        assertTrue(filteredSchema.contains("consentBit"));
    }
}
