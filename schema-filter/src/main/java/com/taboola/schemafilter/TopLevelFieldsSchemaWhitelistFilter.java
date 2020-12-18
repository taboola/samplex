package com.taboola.samplex.schemafilter;

import java.util.Set;

public class TopLevelFieldsSchemaWhitelistFilter extends TopLevelIteratingSchemaFilter {

    private final Set<String> topFieldsToInclude;

    public TopLevelFieldsSchemaWhitelistFilter(Set<String> topFieldsToInclude){

        this.topFieldsToInclude = topFieldsToInclude;
    }

    @Override
    protected boolean shouldFilterField(String name) {
        return !topFieldsToInclude.contains(name);
    }
}
