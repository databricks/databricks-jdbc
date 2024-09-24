package com.databricks.jdbc.integration.benchmarking;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

public class CustomClassLoader extends URLClassLoader {
  private Set<String> packagePrefixes = new HashSet<>();

  public CustomClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    packagePrefixes.add("com.databricks.client.jdbc");
    packagePrefixes.add("com.databricks.client.spark.jdbc");
    packagePrefixes.add("com.databricks.client.hivecommon.jdbc42");
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    for (String prefix : packagePrefixes) {
      if (name.startsWith(prefix)) {
        return findClass(name);
      }
    }
    return super.loadClass(name);
  }
}
