package com.databricks.jdbc.api.impl;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabricksArray implements Array {
    private final String typeName;
    private final Object[] elements;

    public DatabricksArray(String typeName, List<Object> elements) {
        this.typeName = typeName;
        this.elements = convertElements(elements);
    }

    private Object[] convertElements(List<Object> elements) {
        return elements.stream().map(this::convertElement).toArray();
    }

    private Object convertElement(Object element) {
        if (element instanceof Map) {
            return new DatabricksStruct("NestedStruct", (Map<String, Object>) element);
        } else if (element instanceof List) {
            return new DatabricksArray("NestedArray", (List<Object>) element);
        } else {
            return element;
        }
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return this.typeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return java.sql.Types.OTHER; // or another appropriate SQL type
    }

    @Override
    public Object getArray() throws SQLException {
        return this.elements;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return this.getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return java.util.Arrays.copyOfRange(this.elements, (int)index - 1, (int)index - 1 + count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return this.getArray(index, count);
    }

    @Override
    public void free() throws SQLException {
        // No resources to free in this simple implementation
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("getResultSet() not implemented");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(Map<String, Class<?>> map) not implemented");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(long index, int count) not implemented");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
    }
}
