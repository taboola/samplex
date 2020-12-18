package com.taboola.samplex.schemafilter;

import java.util.Set;

public class RecursiveIteratingSchemaWhitelistFilter extends RecursiveIteratingSchemaFilter {

    private Set<String> whitelist;

    public RecursiveIteratingSchemaWhitelistFilter(Set<String> whitelist) {
        this.whitelist = whitelist;
    }

    @Override
    protected boolean shouldFilterField(String name) {
        for (String whitelistItem : whitelist){
            if (name.equals(whitelistItem) || whitelistItem.startsWith(String.format(FIELD_NAME_PATTERN ,name, ""))|| name.startsWith(String.format(FIELD_NAME_PATTERN ,whitelistItem, ""))){
                 return false;
            }
        }
        return true;
    }
}
