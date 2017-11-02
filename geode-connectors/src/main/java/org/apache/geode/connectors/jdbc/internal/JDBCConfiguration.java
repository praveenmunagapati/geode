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
  private static final String JDBC_REGION_SEPARATOR =
      System.getProperty("jdbcRegionSeparator", ":");
  /**
   * syntax: comma separated list of booleanSpecs. booleanSpec: optional regionSpec followed by
   * boolean. regionSpec: regionName followed by a jdbcRegionSeparator. A 'boolean' is parsed by
   * {@link Boolean#parseBoolean(String)}. Whitespace is only allowed around the commas. At most one
   * classSpec without a regionSpec is allowed. A classSpec without a regionSpec defines the
   * default. Only used by JDBCLoader.
   */
  private static final String IS_KEY_PART_OF_VALUE = "isKeyPartOfValue";

  /**
   * syntax: comma separated list of classSpecs. classSpec: optional regionSpec followed by
   * className. regionSpec: regionName followed by a jdbcRegionSeparator. Whitespace is only allowed
   * around the commas. At most one classSpec without a regionSpec is allowed. A classSpec without a
   * regionSpec defines the default. Only used by JDBCLoader.
   */
  private static final String VALUE_CLASS_NAME = "valueClassName";

  private static final List<String> knownProperties = Collections
      .unmodifiableList(Arrays.asList(URL, USER, PASSWORD, VALUE_CLASS_NAME, IS_KEY_PART_OF_VALUE));

  private static final List<String> requiredProperties =
      Collections.unmodifiableList(Arrays.asList(URL));

  private final String url;
  private final String user;
  private final String password;
  private final String valueClassNameDefault;
  private final Map<String, String> regionToClassMap;
  private final boolean keyPartOfValueDefault;
  private final Map<String, Boolean> keyPartOfValueMap;

  public JDBCConfiguration(Properties configProps) {
    validateKnownProperties(configProps);
    validateRequiredProperties(configProps);
    this.url = configProps.getProperty(URL);
    this.user = configProps.getProperty(USER);
    this.password = configProps.getProperty(PASSWORD);
    String valueClassNameProp = configProps.getProperty(VALUE_CLASS_NAME);
    this.valueClassNameDefault = computeDefaultValueClassName(valueClassNameProp);
    this.regionToClassMap = computeRegionToClassMap(valueClassNameProp);
    String keyPartOfValueProp = configProps.getProperty(IS_KEY_PART_OF_VALUE);
    this.keyPartOfValueDefault = computeDefaultKeyPartOfValue(keyPartOfValueProp);
    this.keyPartOfValueMap = computeKeyPartOfValueMap(keyPartOfValueProp);
  }

  private String computeDefaultValueClassName(String valueClassNameProp) {
    return parseDefault(VALUE_CLASS_NAME, valueClassNameProp, v -> {
      return v;
    }, null);
  }

  private Map<String, String> computeRegionToClassMap(String valueClassNameProp) {
    return parseMap(valueClassNameProp, v -> {
      return v;
    });
  }

  private boolean computeDefaultKeyPartOfValue(String keyPartOfValueProp) {
    return parseDefault(IS_KEY_PART_OF_VALUE, keyPartOfValueProp, v -> {
      return Boolean.parseBoolean(v);
    }, false);
  }

  private Map<String, Boolean> computeKeyPartOfValueMap(String keyPartOfValueProp) {
    return parseMap(keyPartOfValueProp, v -> {
      return Boolean.parseBoolean(v);
    });
  }

  public interface ValueParser<V> {
    public V parseValue(String valueString);
  }

  private <V> Map<String, V> parseMap(String propertyValue, ValueParser<V> parser) {
    if (propertyValue == null) {
      return null;
    }
    Map<String, V> result = new HashMap<>();
    List<String> items = Arrays.asList(propertyValue.split("\\s*,\\s*"));
    for (String item : items) {
      int idx = item.indexOf(getJdbcRegionSeparator());
      if (idx == -1) {
        continue;
      }
      String regionName = item.substring(0, idx);
      String valueString = item.substring(idx + getJdbcRegionSeparator().length());;
      result.put(regionName, parser.parseValue(valueString));
    }
    return result;
  }

  private <V> V parseDefault(String propertyName, String propertyValue, ValueParser<V> parser,
      V defaultValue) {
    if (propertyValue == null) {
      return defaultValue;
    }
    V result = null;
    List<String> items = Arrays.asList(propertyValue.split("\\s*,\\s*"));
    for (String item : items) {
      int idx = item.indexOf(getJdbcRegionSeparator());
      if (idx != -1) {
        continue;
      }
      if (result != null) {
        throw new IllegalArgumentException(
            propertyName + " can have at most one item that does not have a "
                + getJdbcRegionSeparator() + " in it.");
      }
      result = parser.parseValue(item);
    }
    if (result == null) {
      result = defaultValue;
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
      return this.valueClassNameDefault;
    }
    String result = this.regionToClassMap.get(regionName);
    if (result == null) {
      result = this.valueClassNameDefault;
    }
    return result;
  }

  public boolean getIsKeyPartOfValue(String regionName) {
    if (this.keyPartOfValueMap == null) {
      return this.keyPartOfValueDefault;
    }
    Boolean result = this.keyPartOfValueMap.get(regionName);
    if (result == null) {
      return this.keyPartOfValueDefault;
    }
    return result;
  }

  protected String getJdbcRegionSeparator() {
    return JDBC_REGION_SEPARATOR;
  }

}
