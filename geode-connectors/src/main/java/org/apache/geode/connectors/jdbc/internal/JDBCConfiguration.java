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
import java.util.function.Function;
import java.util.function.IntPredicate;

public class JDBCConfiguration {
  private static final String URL = "url";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private static final String JDBC_SEPARATOR = System.getProperty("jdbcSeparator", ":");
  /**
   * syntax: comma separated list of booleanSpecs. booleanSpec: optional regionSpec followed by
   * boolean. regionSpec: regionName followed by a jdbcSeparator. A 'boolean' is parsed by
   * {@link Boolean#parseBoolean(String)}. Whitespace is only allowed around the commas. At most one
   * classSpec without a regionSpec is allowed. A classSpec without a regionSpec defines the
   * default. Only used by JDBCLoader.
   */
  private static final String IS_KEY_PART_OF_VALUE = "isKeyPartOfValue";

  /**
   * syntax: comma separated list of classSpecs. classSpec: optional regionSpec followed by
   * className. regionSpec: regionName followed by a jdbcSeparator. Whitespace is only allowed
   * around the commas. At most one classSpec without a regionSpec is allowed. A classSpec without a
   * regionSpec defines the default. Only used by JDBCLoader.
   */
  private static final String VALUE_CLASS_NAME = "valueClassName";

  /**
   * syntax: comma separated list of regionTableSpecs. regionTableSpecs: regionName followed by
   * jdbcSeparator followed by tableName. Whitespace is only allowed around the commas.
   */
  private static final String REGION_TO_TABLE = "regionToTable";

  /**
   * syntax: comma separated list of fieldColumnSpecs. fieldColumnSpecs: Optional regionSpec
   * followed by fieldName followed by jdbcSeparator followed by columnName. regionSpec: regionName
   * followed by jdbcSeparator. Whitespace is only allowed around the commas.
   */
  private static final String FIELD_TO_COLUMN = "fieldToColumn";

  private static final List<String> knownProperties =
      Collections.unmodifiableList(Arrays.asList(URL, USER, PASSWORD, VALUE_CLASS_NAME,
          IS_KEY_PART_OF_VALUE, REGION_TO_TABLE, FIELD_TO_COLUMN));

  private static final List<String> requiredProperties =
      Collections.unmodifiableList(Arrays.asList(URL));

  private final String url;
  private final String user;
  private final String password;
  private final String valueClassNameDefault;
  private final Map<String, String> regionToClassMap;
  private final boolean keyPartOfValueDefault;
  private final Map<String, Boolean> keyPartOfValueMap;
  private final Map<String, String> regionToTableMap;
  private final Map<RegionField, String> fieldToColumnMap;

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
    this.regionToTableMap = computeRegionToTableMap(configProps.getProperty(REGION_TO_TABLE));
    this.fieldToColumnMap = computeFieldToColumnMap(configProps.getProperty(FIELD_TO_COLUMN));
  }

  public static class RegionField {
    private final String regionName; // may be null
    private final String fieldName;

    public RegionField(String regionName, String fieldName) {
      this.regionName = regionName;
      this.fieldName = fieldName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + fieldName.hashCode();
      result = prime * result + ((regionName == null) ? 0 : regionName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RegionField other = (RegionField) obj;
      if (!fieldName.equals(other.fieldName)) {
        return false;
      }
      if (regionName == null) {
        if (other.regionName != null) {
          return false;
        }
      } else if (!regionName.equals(other.regionName)) {
        return false;
      }
      return true;
    }
  }

  private Map<RegionField, String> computeFieldToColumnMap(String prop) {
    Function<String, RegionField> regionFieldParser = new Function<String, RegionField>() {
      @Override
      public RegionField apply(String item) {
        String regionName = null;
        String fieldName;
        int idx = item.indexOf(getjdbcSeparator());
        if (idx != -1) {
          regionName = item.substring(0, idx);
          fieldName = item.substring(idx + getjdbcSeparator().length());
        } else {
          fieldName = item;
        }
        return new RegionField(regionName, fieldName);
      }
    };
    return parseMap(prop, regionFieldParser, v -> v, true);
  }

  private Map<String, String> computeRegionToTableMap(String prop) {
    return parseMap(prop, k -> k, v -> v, true);
  }

  private String computeDefaultValueClassName(String valueClassNameProp) {
    return parseDefault(VALUE_CLASS_NAME, valueClassNameProp, v -> v, null);
  }

  private Map<String, String> computeRegionToClassMap(String valueClassNameProp) {
    return parseMap(valueClassNameProp, k -> k, v -> v, false);
  }

  private boolean computeDefaultKeyPartOfValue(String keyPartOfValueProp) {
    return parseDefault(IS_KEY_PART_OF_VALUE, keyPartOfValueProp, Boolean::parseBoolean, false);
  }

  private Map<String, Boolean> computeKeyPartOfValueMap(String keyPartOfValueProp) {
    return parseMap(keyPartOfValueProp, k -> k, Boolean::parseBoolean, false);
  }

  private <K, V> Map<K, V> parseMap(String propertyValue, Function<String, K> keyParser,
      Function<String, V> valueParser, boolean failOnNoSeparator) {
    if (propertyValue == null) {
      return null;
    }
    Map<K, V> result = new HashMap<>();
    List<String> items = Arrays.asList(propertyValue.split("\\s*,\\s*"));
    for (String item : items) {
      int idx = item.lastIndexOf(getjdbcSeparator());
      if (idx == -1) {
        if (failOnNoSeparator) {
          throw new IllegalArgumentException(item + " does not contain " + getjdbcSeparator());
        }
        continue;
      }
      String keyString = item.substring(0, idx);
      String valueString = item.substring(idx + getjdbcSeparator().length());
      K key = keyParser.apply(keyString);
      if (result.containsKey(key)) {
        throw new IllegalArgumentException("Duplicate item " + key + " is not allowed.");
      }
      result.put(key, valueParser.apply(valueString));
    }
    return result;
  }

  private <V> V parseDefault(String propertyName, String propertyValue, Function<String, V> parser,
      V defaultValue) {
    if (propertyValue == null) {
      return defaultValue;
    }
    V result = null;
    List<String> items = Arrays.asList(propertyValue.split("\\s*,\\s*"));
    for (String item : items) {
      int idx = item.indexOf(getjdbcSeparator());
      if (idx != -1) {
        continue;
      }
      if (result != null) {
        throw new IllegalArgumentException(propertyName
            + " can have at most one item that does not have a " + getjdbcSeparator() + " in it.");
      }
      result = parser.apply(item);
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

  protected String getjdbcSeparator() {
    return JDBC_SEPARATOR;
  }

  public String getTableForRegion(String regionName) {
    if (this.regionToTableMap == null) {
      return regionName;
    }
    String result = this.regionToTableMap.get(regionName);
    if (result == null) {
      result = regionName;
    }
    return result;
  }

  public String getColumnForRegionField(String regionName, String fieldName) {
    if (this.fieldToColumnMap == null) {
      return fieldName;
    }
    RegionField key = new RegionField(regionName, fieldName);
    String result = this.fieldToColumnMap.get(key);
    if (result == null) {
      key = new RegionField(null, fieldName);
      result = this.fieldToColumnMap.get(key);
      if (result == null) {
        result = regionName;
      }
    }
    return result;
  }
}
