package com.taboola.samplex.schemafilter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public abstract class TopLevelIteratingSchemaFilter implements SchemaFilter {

    private static final Logger logger = LogManager.getLogger(TopLevelIteratingSchemaFilter.class);

    @Override
    public String filter(String schema) {
        try {
            JSONObject schemaJson = new JSONObject(schema);
            JSONArray fieldsJson = schemaJson.getJSONArray("fields");
            JSONArray newFieldsJson = new JSONArray();

            for (int i = 0; i < fieldsJson.length(); i++) {
                JSONObject field = fieldsJson.getJSONObject(i);
                if (!shouldFilterField(field.getString("name"))) {
                    newFieldsJson.put(field);
                }
            }
            schemaJson.put("fields", newFieldsJson);
            return schemaJson.toString();
        } catch (JSONException e) {
            logger.error("Error while blacklisting schema. schema: " + schema, e);
            return schema;
        }
    }

    protected abstract boolean shouldFilterField(String name);
}
