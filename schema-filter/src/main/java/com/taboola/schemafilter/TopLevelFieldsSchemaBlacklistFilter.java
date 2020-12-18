package com.taboola.samplex.schemafilter;

import java.util.Set;

public class TopLevelFieldsSchemaBlacklistFilter extends TopLevelIteratingSchemaFilter {
    private final Set<String> topFieldsToExclude;

    public TopLevelFieldsSchemaBlacklistFilter(Set<String> topFieldsToExclude) {

        this.topFieldsToExclude = topFieldsToExclude;
    }

    @Override
    protected boolean shouldFilterField(String name) {
        return topFieldsToExclude.contains(name);
    }
}
