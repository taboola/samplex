package com.taboola.schemafilter;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class RecursiveIteratingSchemaFilter implements SchemaFilter {

    private static final Logger logger = LogManager.getLogger(RecursiveIteratingSchemaFilter.class);
    private static final String FIELDS_NAME = "fields";
    private static final String TYPE_NAME = "type";
    protected static final String FIELD_NAME_PATTERN = "%s.%s";
    private static final String RECORD_TYPE = "record";
    private static final String ARRAY_TYPE = "array";
    private static final String MAP_TYPE = "map";
    private static final String ITEMS_NAME = "items";
    private static final String VALUES_NAME = "values";

    @Override
    public String filter(String schema) {
        try {
            JSONObject schemaJson = new JSONObject(schema);
            return filterFields(schemaJson).toString();
        } catch (Throwable e) {
            logger.error("Error while blacklisting schema, returning original. schema: " + schema, e);
            return schema;
        }
    }
    private JSONObject filterFields(JSONObject schema) throws JSONException {
        return filterRecordFields(schema, "");
    }

    private JSONObject filterRecordFields(JSONObject schema, String fieldNamePrefix) throws JSONException {
        JSONArray fieldsJson = schema.getJSONArray(FIELDS_NAME);
        JSONArray newFieldsJson = new JSONArray();
        for (int i = 0; i < fieldsJson.length(); i++) {
            JSONObject field = fieldsJson.getJSONObject(i);

            String fieldName = field.getString("name");
            String recursiveFieldName = getFullFieldName(fieldName, fieldNamePrefix);
            if (!shouldFilterField(recursiveFieldName)) {
                field = filterInnerType(field, recursiveFieldName, TYPE_NAME);
                newFieldsJson.put(field);
            }
        }
        schema.put("fields", newFieldsJson);

        return schema;
    }

    private String getFullFieldName(String name, String prefix){
        if (StringUtils.isEmpty(prefix)){
            return name;
        }

        return String.format(FIELD_NAME_PATTERN, prefix, name);
    }

    private JSONObject filterByType(JSONObject field, String recursiveFieldName) throws JSONException {
        String actualType = field.getString(TYPE_NAME);

        JSONObject filtered = field;
        switch (actualType) {
            case RECORD_TYPE:
                filtered = filterRecordFields(field, recursiveFieldName);
                break;
            case ARRAY_TYPE:
                filtered = filterInnerType(field, recursiveFieldName, ITEMS_NAME);
                break;
            case MAP_TYPE:
                filtered = filterInnerType(field, recursiveFieldName, VALUES_NAME);
                break;
        }

        return filtered;
    }

    private JSONObject filterInnerType(JSONObject typeObject, String recursiveFieldName, String innerTypeFieldName) throws JSONException {
        Object innerTypes = typeObject.get(innerTypeFieldName);
        if (innerTypes instanceof JSONArray) { //this is a UNION field
            JSONArray innerTypesArray = (JSONArray)innerTypes;

            int actualTypeIndex = getIndexOfFirstNonPrimitive(innerTypesArray);
            if (actualTypeIndex == -1) {
                return typeObject;
            }

            JSONObject filtered = filterByType(innerTypesArray.getJSONObject(actualTypeIndex), recursiveFieldName);
            innerTypesArray.put(actualTypeIndex, filtered);
            return typeObject;
        }
        else if (innerTypes instanceof JSONObject){
            JSONObject  filtered = filterByType((JSONObject)innerTypes, recursiveFieldName);
            typeObject.put(innerTypeFieldName, filtered);
        }

        return typeObject;
    }

    private int getIndexOfFirstNonPrimitive(JSONArray jsonArray) throws JSONException {
        for (int i = 0; i < jsonArray.length(); i ++){
            Object item = jsonArray.get(i);
            if (item instanceof JSONObject){
                return i;
            }
        }
        return -1;
    }

    protected abstract boolean shouldFilterField(String name);
}
