package com.taboola.taz.analyzers.utils;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.taboola.taz.analyzers.utils.schemafilter.TopLevelFieldsSchemaBlacklistFilter;
import com.taboola.testing.BaseUnitTest;

public class TopLevelFieldsSchemaBlacklistFilterTest extends BaseUnitTest {
    private static String schema = "{\"type\":\"record\",\"name\":\"spark_schema\",\"fields\":[{\"name\":\"pv_commonGeoRegionId\",\"type\":[\"long\",\"null\"]},{\"name\":\"pv_consentDaisyBit\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_country\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_countryCode\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_crossDeviceControlGroup\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"pv_ctrModelConfigLabel\",\"type\":[\"string\",\"null\"]},{\"name\":\"pv_deviceId\",\"type\":[\"string\",\"null\"]}]}";
    private JSONObject jsonSchema = new JSONObject(schema);

    public TopLevelFieldsSchemaBlacklistFilterTest() throws JSONException {
    }

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
        String fieldToFilter = "pv_commonGeoRegionId";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter);
        TopLevelFieldsSchemaBlacklistFilter filter = new TopLevelFieldsSchemaBlacklistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertNotEquals(jsonSchema.toString(), filteredJson.toString());
        assertFalse(filteredSchema.contains(fieldToFilter));
        assertTrue(filteredSchema.contains("pv_consentDaisyBit"));
    }
}
