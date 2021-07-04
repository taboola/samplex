package com.taboola.schemafilter;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class TopLevelFieldsSchemaWhitelistFilterTest {
    private static final String schema = "{\"type\":\"record\",\"name\":\"spark_schema\",\"fields\":[{\"name\":\"geoRegionId\",\"type\":[\"long\",\"null\"]},{\"name\":\"consentBit\",\"type\":[\"string\",\"null\"]},{\"name\":\"country\",\"type\":[\"string\",\"null\"]},{\"name\":\"countryCode\",\"type\":[\"string\",\"null\"]},{\"name\":\"isControlGroup\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"configLabel\",\"type\":[\"string\",\"null\"]},{\"name\":\"deviceId\",\"type\":[\"string\",\"null\"]}]}";

    @Test
    public void filter_filterSetIsEmpty_returnNoFields() throws JSONException {
        TopLevelFieldsSchemaWhitelistFilter filter = new TopLevelFieldsSchemaWhitelistFilter(new HashSet<>());
        String filteredSchema = filter.filter(schema);
        JSONArray filteredJson = new JSONObject(filteredSchema).getJSONArray("fields");
        assertEquals(0, filteredJson.length());
    }

    @Test
    public void filter_schemaHasNoFieldsFromFilterSet__returnNoFields() throws JSONException {
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add("doesNotExist2");
        TopLevelFieldsSchemaWhitelistFilter filter = new TopLevelFieldsSchemaWhitelistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONArray filteredJson = new JSONObject(filteredSchema).getJSONArray("fields");
        assertEquals(0, filteredJson.length());
    }

    @Test
    public void filter_schemaHasFieldsFromFilterSet_returnFiltered() throws JSONException {
        String fieldToFilter = "geoRegionId";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter);
        TopLevelFieldsSchemaWhitelistFilter filter = new TopLevelFieldsSchemaWhitelistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONArray filteredJson = new JSONObject(filteredSchema).getJSONArray("fields");
        assertEquals(1, filteredJson.length());
        assertEquals(fieldToFilter, filteredJson.getJSONObject(0).getString("name"));
    }
}
