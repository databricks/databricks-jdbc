package com.databricks.jdbc.api.impl;

import java.sql.Struct;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabricksStruct implements Struct {
    private final String typeName;
    private final Object[] attributes;

    public DatabricksStruct(String typeName, Map<String, Object> attributes) {
        this.typeName = typeName;
        this.attributes = convertAttributes(attributes);
    }

    private Object[] convertAttributes(Map<String, Object> attributes) {
        return attributes.values().stream().map(this::convertAttribute).toArray();
    }

    private Object convertAttribute(Object attribute) {
        if (attribute instanceof Map) {
            return new DatabricksStruct("NestedStruct", (Map<String, Object>) attribute);
        } else if (attribute instanceof List) {
            return new DatabricksArray("NestedArray", (List<Object>) attribute);
        } else {
            return attribute;
        }
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return this.typeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return this.attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return this.getAttributes();
    }
}
