package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpHeader {
    @JsonProperty("name")
    private String name;

    @JsonProperty("value")
    private String value;

    // Getters
    public String getName(){
        return this.name;
    }

    public String getValue()
    {
        return this.value;
    }

    // Setters
    public String setName(String name)
    {
        return this.name=name;
    }

    public String setValue(String value)
    {
        return this.value=value;
    }

    // Override toString method
    @Override
    public String toString() {
        return "HttpHeader{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
