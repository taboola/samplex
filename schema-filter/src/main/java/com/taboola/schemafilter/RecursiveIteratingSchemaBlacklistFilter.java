package com.taboola.samplex.schemafilter;

import java.util.Set;

public class RecursiveIteratingSchemaBlacklistFilter extends RecursiveIteratingSchemaFilter {

    private Set<String> blacklist;

    public RecursiveIteratingSchemaBlacklistFilter(Set<String> blacklist) {
        this.blacklist = blacklist;
    }

    @Override
    protected boolean shouldFilterField(String name) {
        for (String blacklistItem : blacklist) {
            if (name.equals(blacklistItem) || name.startsWith(String.format(FIELD_NAME_PATTERN, blacklistItem, ""))) {
                return true;
            }
        }
        return false;
    }
}
