package com.databricks.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

public class ProxyTest {
    public static void main(String[] args) {
        String proxyUrl = null;
        String patToken = null;
        String host = null;
        String httpPath = null;

        for (String arg : args) {
            if (arg.startsWith("--proxy=")) {
                proxyUrl = arg.substring("--proxy=".length());
            } else if (arg.startsWith("--pat=")) {
                patToken = arg.substring("--pat=".length());
            }
            else if(arg.startsWith("--host=")) {
                host = arg.substring("--host=".length());
            }
            else if(arg.startsWith("--http-path=")) {
                httpPath = arg.substring("--http-path=".length());
            }
        }

        // If we got a proxy URL, set system properties
        if (proxyUrl != null) {
            String[] parts = proxyUrl.replace("http://","").split(":");
            String proxyHost = parts[0];
            String proxyPort = parts.length > 1 ? parts[1] : "3128";
            System.setProperty("http.proxyHost", proxyHost);
            System.setProperty("http.proxyPort", proxyPort);
            System.setProperty("https.proxyHost", proxyHost);
            System.setProperty("https.proxyPort", proxyPort);
        }

        System.out.println("ProxyTest starting with: " + proxyUrl + " / PAT: " + patToken);

        String jdbcUrl = "jdbc:databricks://" + host + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath=" + httpPath + ";";

        try (Connection connection = DriverManager.getConnection(jdbcUrl, "token", args[0])) {
            System.out.println("JDBC connection through proxy succeeded.");
        } catch (Exception e) {
            System.err.println("JDBC proxy test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

