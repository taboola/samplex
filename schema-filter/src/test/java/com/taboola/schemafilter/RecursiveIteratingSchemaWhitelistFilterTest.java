package com.taboola.taz.analyzers.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;


import com.taboola.taz.analyzers.utils.schemafilter.RecursiveIteratingSchemaWhitelistFilter;
import com.taboola.testing.BaseUnitTest;
import com.taboola.utils.FileUtils;

public class RecursiveIteratingSchemaWhitelistFilterTest extends BaseUnitTest {

    private static String schema = getSchema();
    private JSONObject jsonSchema = new JSONObject(schema);

    public RecursiveIteratingSchemaWhitelistFilterTest() throws JSONException {
    }

    @Test
    public void filter_filterSetIsEmpty_returnNoFields() throws JSONException {
        RecursiveIteratingSchemaWhitelistFilter filter = new RecursiveIteratingSchemaWhitelistFilter(new HashSet<>());
        String filteredSchema = filter.filter(schema);
        JSONArray filteredJson = new JSONObject(filteredSchema).getJSONArray("fields");
        assertEquals(0, filteredJson.length());
    }

    @Test
    public void filter_schemaHasNoFieldsFromFilterSet_returnNoFields() throws JSONException {
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add("doesNotExist2");
        RecursiveIteratingSchemaWhitelistFilter filter = new RecursiveIteratingSchemaWhitelistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONArray filteredJson = new JSONObject(filteredSchema).getJSONArray("fields");
        assertEquals(0, filteredJson.length());
    }

    @Test
    public void filter_TopLevelSchemaFilterSet_returnFiltered() throws JSONException {
        String fieldToFilter = "servedItem";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter);
        RecursiveIteratingSchemaWhitelistFilter filter = new RecursiveIteratingSchemaWhitelistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONArray filteredJson = new JSONObject(filteredSchema).getJSONArray("fields");
        assertEquals(1, filteredJson.length());
        assertEquals(fieldToFilter, filteredJson.getJSONObject(0).getString("name"));

        Schema schema = new Schema.Parser().parse(filteredSchema); // sanity
        List<Schema.Field> filtered = schema.getField("servedItem").schema().getTypes().get(0).getField("creativeInfo_creativeComponentData").schema().getTypes().get(0).getValueType().getTypes().get(0).getFields();
        assertEquals(2, filtered.size());
    }

    @Test
    public void filter_FilterRecursiveLevel_returnFiltered() throws JSONException {
        String fieldToFilter1 = "pv_performanceMeasurements.type";
        String fieldToFilter2 = "servedItem.creativeInfo_creativeComponentData.additionalData";
        String fieldToFilter3 = "servedItem.alchemySentiment";
        String fieldToFilter4 = "pv_pageViewKey_pageViewUniqueId";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter1);
        toFilter.add(fieldToFilter2);
        toFilter.add(fieldToFilter3);
        toFilter.add(fieldToFilter4);
        RecursiveIteratingSchemaWhitelistFilter filter = new RecursiveIteratingSchemaWhitelistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertNotEquals(jsonSchema.toString(), filteredJson.toString());
        Schema schema = new Schema.Parser().parse(filteredSchema);

        assertEquals(3, schema.getFields().size());

        List<Schema.Field> fields = schema.getField("pv_performanceMeasurements").schema().getTypes().get(0).getValueType().getTypes().get(0).getElementType().getTypes().get(0).getFields();
        assertEquals(1, fields.size());
        assertEquals("type", fields.get(0).name());

        fields = schema.getField("servedItem").schema().getTypes().get(0).getFields();
        assertEquals(2, fields.size());

        assertEquals("alchemySentiment", fields.get(0).name());

        fields = fields.get(1).schema().getTypes().get(0).getValueType().getTypes().get(0).getFields();
        assertEquals(1, fields.size());
        assertEquals("additionalData", fields.get(0).name());

        assertNotNull(schema.getField(fieldToFilter4));
    }
    
    private static String getSchema() {
        String schema = FileUtils.readFileFromClasspath("schema-filter/testSchema.json");
        return schema;
    }
}
