/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.IntPredicate;

public class JDBCConfiguration {
  private static final String URL = "url";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  /**
   * syntax: comma separated list of classSpecs. classSpec: optional regionName followed by
   * className
   */
  private static final String VALUE_CLASS_NAME = "valueClassName";

  private static final List<String> knownProperties =
      Collections.unmodifiableList(Arrays.asList(URL, USER, PASSWORD, VALUE_CLASS_NAME));

  private static final List<String> requiredProperties =
      Collections.unmodifiableList(Arrays.asList(URL));

  private final String url;
  private final String user;
  private final String password;
  private final String valueClassName;
  private final Map<String, String> regionToClassMap;

  public JDBCConfiguration(Properties configProps) {
    validateKnownProperties(configProps);
    validateRequiredProperties(configProps);
    this.url = configProps.getProperty(URL);
    this.user = configProps.getProperty(USER);
    this.password = configProps.getProperty(PASSWORD);
    String valueClassNameProp = configProps.getProperty(VALUE_CLASS_NAME);
    this.valueClassName = computeValueClassName(valueClassNameProp);
    this.regionToClassMap = computeRegionToClassMap(valueClassNameProp);
  }

  private static Map<String, String> computeRegionToClassMap(String valueClassNameProp) {
    if (valueClassNameProp == null) {
      return null;
    }
    Map<String, String> result = new HashMap<>();
    List<String> items = Arrays.asList(valueClassNameProp.split("\\s*,\\s*"));
    for (String item : items) {
      int idx = item.indexOf(':');
      if (idx == -1) {
        continue;
      }
      String regionName = item.substring(0, idx);
      String className = item.substring(idx + 1);
      result.put(regionName, className);
    }
    return result;
  }

  private static String computeValueClassName(String valueClassNameProp) {
    if (valueClassNameProp == null) {
      return null;
    }
    String result = null;
    List<String> items = Arrays.asList(valueClassNameProp.split("\\s*,\\s*"));
    for (String item : items) {
      if (item.indexOf(':') != -1) {
        continue;
      }
      if (result != null) {
        throw new IllegalArgumentException(
            VALUE_CLASS_NAME + " can have at most one item that does not have a ':' in it.");
      }
      result = item;
    }
    return result;
  }

  private void validateKnownProperties(Properties configProps) {
    Set<Object> keys = new HashSet<>(configProps.keySet());
    keys.removeAll(knownProperties);
    if (!keys.isEmpty()) {
      throw new IllegalArgumentException("unknown properties: " + keys);
    }
  }

  private void validateRequiredProperties(Properties configProps) {
    List<String> reqKeys = new ArrayList<>(requiredProperties);
    reqKeys.removeAll(configProps.keySet());
    if (!reqKeys.isEmpty()) {
      Collections.sort(reqKeys);
      throw new IllegalArgumentException("missing required properties: " + reqKeys);
    }
  }

  public String getURL() {
    return this.url;
  }

  public String getUser() {
    return this.user;
  }

  public String getPassword() {
    return this.password;
  }

  public String getValueClassName(String regionName) {
    if (this.regionToClassMap == null) {
      return this.valueClassName;
    }
    String result = this.regionToClassMap.get(regionName);
    if (result == null) {
      result = this.valueClassName;
    }
    return result;
  }
}
