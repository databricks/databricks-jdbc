package com.databricks.jdbc.api.impl;

import java.sql.Array;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DatabricksArray implements Array {
    private final Object[] elements;
    private final String typeName;

    public DatabricksArray(List<Object> elements, String metadata) {
        String elementType = MetadataParser.parseArrayMetadata(metadata);
        this.elements = convertElements(elements, elementType);
        this.typeName = metadata;
    }

    private Object[] convertElements(List<Object> elements, String elementType) {
        Object[] convertedElements = new Object[elements.size()];

        for (int i = 0; i < elements.size(); i++) {
            Object element = elements.get(i);

            if (elementType.startsWith("STRUCT")) {
                if (element instanceof Map) {
                    convertedElements[i] = new DatabricksStruct((Map<String, Object>) element, elementType);
                } else {
                    throw new IllegalArgumentException("Expected a Map for STRUCT but found: " + element.getClass().getSimpleName());
                }
            } else if (elementType.startsWith("ARRAY")) {
                if (element instanceof List) {
                    convertedElements[i] = new DatabricksArray((List<Object>) element, elementType);
                } else {
                    throw new IllegalArgumentException("Expected a List for ARRAY but found: " + element.getClass().getSimpleName());
                }
            } else if (elementType.startsWith("MAP")) {
                if (element instanceof Map) {
                    convertedElements[i] = new DatabricksMap<>((Map<String, Object>) element, elementType);
                } else {
                    throw new IllegalArgumentException("Expected a Map for MAP but found: " + element.getClass().getSimpleName());
                }
            } else {
                // Properly convert value based on type
                convertedElements[i] = convertValue(element, elementType);
            }
        }
        return convertedElements;
    }

    private Object convertValue(Object value, String type) {
        if (value == null) return null;
        switch (type.toUpperCase()) {
            case "INT":
            case "INTEGER":
                return Integer.parseInt(value.toString());
            case "FLOAT":
                return Float.parseFloat(value.toString());
            case "DOUBLE":
                return Double.parseDouble(value.toString());
            case "BOOLEAN":
                return Boolean.parseBoolean(value.toString());
            case "STRING":
            default:
                return value.toString();
        }
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return this.typeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return java.sql.Types.OTHER;  // Or appropriate SQL type
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
        return java.util.Arrays.copyOfRange(this.elements, (int) index - 1, (int) index - 1 + count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return this.getArray(index, count);
    }

    @Override
    public void free() throws SQLException {
        // No resources to free in this implementation
    }

    @Override
    public java.sql.ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("getResultSet() not implemented");
    }

    @Override
    public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(Map<String, Class<?>> map) not implemented");
    }

    @Override
    public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(long index, int count) not implemented");
    }

    @Override
    public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
    }
}
