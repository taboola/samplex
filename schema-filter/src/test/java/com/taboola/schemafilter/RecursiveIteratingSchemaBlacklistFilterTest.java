package com.taboola.taz.analyzers.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import com.taboola.taz.analyzers.utils.schemafilter.RecursiveIteratingSchemaBlacklistFilter;
import com.taboola.testing.BaseUnitTest;
import com.taboola.utils.FileUtils;

public class RecursiveIteratingSchemaBlacklistFilterTest extends BaseUnitTest {

    private static String schema = getSchema();
    private JSONObject jsonSchema = new JSONObject(schema);

    public RecursiveIteratingSchemaBlacklistFilterTest() throws JSONException {
    }

    @Test
    public void filter_filterSetIsEmpty_returnUntouched() throws JSONException {
        RecursiveIteratingSchemaBlacklistFilter filter = new RecursiveIteratingSchemaBlacklistFilter(new HashSet<>());
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertEquals(jsonSchema.toString(), filteredJson.toString());
    }

    @Test
    public void filter_schemaHasNoFieldsFromFilterSet_returnUntouched() throws JSONException {
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add("doesNotExist2.blabla");
        toFilter.add("servedItem.doesnotExist");
        RecursiveIteratingSchemaBlacklistFilter filter = new RecursiveIteratingSchemaBlacklistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertEquals(jsonSchema.toString(), filteredJson.toString());
    }

    @Test
    public void filter_FilterTopLevel_returnFiltered() throws JSONException {
        String fieldToFilter = "servedItem";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter);
        RecursiveIteratingSchemaBlacklistFilter filter = new RecursiveIteratingSchemaBlacklistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertNotEquals(jsonSchema.toString(), filteredJson.toString());

        Schema originalSchema = new Schema.Parser().parse(schema);
        Schema filteredParsed = new Schema.Parser().parse(filteredSchema);
        assertEquals(originalSchema.getFields().size() -1, filteredParsed.getFields().size());
        assertNull(filteredParsed.getField(fieldToFilter));
    }

    @Test
    public void filter_FilterRecursiveLevel_returnFiltered() throws JSONException {
        String fieldToFilter1 = "pv_performanceMeasurements.type";
        String fieldToFilter2 = "servedItem.creativeInfo_creativeComponentData.additionalData";
        Set<String> toFilter = new HashSet<>();
        toFilter.add("doesNotExist1");
        toFilter.add(fieldToFilter1);
        toFilter.add(fieldToFilter2);
        RecursiveIteratingSchemaBlacklistFilter filter = new RecursiveIteratingSchemaBlacklistFilter(toFilter);
        String filteredSchema = filter.filter(schema);
        JSONObject filteredJson = new JSONObject(filteredSchema);
        assertNotEquals(jsonSchema.toString(), filteredJson.toString());

        Schema testSchema = new Schema.Parser().parse(filteredSchema);
        Schema originalSchema = new Schema.Parser().parse(schema);

        Schema field = testSchema.getField("pv_performanceMeasurements").schema().getTypes().get(0).getValueType().getTypes().get(0).getElementType().getTypes().get(0);
        Schema originalField = originalSchema.getField("pv_performanceMeasurements").schema().getTypes().get(0).getValueType().getTypes().get(0).getElementType().getTypes().get(0);
        assertNull(field.getField("type"));
        assertEquals(originalField.getFields().size() -1, field.getFields().size());

        List<Schema.Field> fields = testSchema.getField("servedItem").schema().getTypes().get(0).getField("creativeInfo_creativeComponentData").schema().getTypes().get(0).getValueType().getTypes().get(0).getFields();
        assertEquals(1, fields.size());
        assertNotEquals("additionalData", fields.get(0).name());

        assertEquals(originalSchema.getFields().size(), testSchema.getFields().size());
    }

    private static String getSchema() {
        String schema = FileUtils.readFileFromClasspath("schema-filter/testSchema.json");
        return schema;
    }
}
