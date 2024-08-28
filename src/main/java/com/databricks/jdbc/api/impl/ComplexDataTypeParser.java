package com.databricks.jdbc.api.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Array;
import java.sql.Struct;
import java.util.*;

public class ComplexDataTypeParser {
    private final ObjectMapper objectMapper;

    public ComplexDataTypeParser() {
        this.objectMapper = new ObjectMapper();
    }

    public Object parseToJavaObject(JsonNode node) {
        if (node.isObject()) {
            return parseToStruct(node);
        } else if (node.isArray()) {
            return parseToArray(node);
        } else if (node.isValueNode()) {
            return node.asText();
        }
        return null;
    }

    private Map<String, Object> parseToStruct(JsonNode node) {
        Map<String, Object> structMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            structMap.put(entry.getKey(), parseToJavaObject(entry.getValue()));
        }
        return structMap;
    }

    private List<Object> parseToArray(JsonNode node) {
        List<Object> arrayList = new ArrayList<>();
        for (JsonNode element : node) {
            arrayList.add(parseToJavaObject(element));
        }
        return arrayList;
    }

    public Map<String, Object> parseToMap(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            return parseToStruct(node);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON: " + json, e);
        }
    }
}
