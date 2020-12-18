package com.taboola.taz.analyzers.utils;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.taboola.taz.analyzers.utils.schemafilter.TopLevelFieldsSchemaWhitelistFilter;
import com.taboola.testing.BaseUnitTest;

public class TopLevelFieldsSchemaWhitelistFilterTest extends BaseUnitTest {
    private static String schema = "{\"type\":\"record\",\"name\":\"spark_schema\",\"fields\":[{\"name\":\"pv_commonGeoRegionId\",\"type\":[\"long\",\"null\"]},{\"name\":\"pv_consentDaisyBit\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_country\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_countryCode\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_crossDeviceControlGroup\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"pv_ctrModelConfigLabel\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_deviceId\",\"type\":[\"string\",\"null\"]}]}";

    public TopLevelFieldsSchemaWhitelistFilterTest() throws JSONException {
    }

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
        String fieldToFilter = "pv_commonGeoRegionId";
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
