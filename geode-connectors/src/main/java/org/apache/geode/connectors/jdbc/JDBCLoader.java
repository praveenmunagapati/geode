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
package org.apache.geode.connectors.jdbc;

import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.connectors.jdbc.internal.JDBCConfiguration;
import org.apache.geode.connectors.jdbc.internal.JDBCManager;

import java.util.Properties;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;

/*
 * This class provides loading from a data source using JDBC.
 *
 * @since Geode 1.4
 */
public class JDBCLoader<K, V> implements CacheLoader<K, V> {
  private JDBCManager manager;

  @Override
  public void close() {
    if (this.manager != null) {
      this.manager.close();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  /**
   * @return this method always returns a PdxInstance. It does not matter what the V generic
   *         parameter is set to.
   */
  public V load(LoaderHelper<K, V> helper) throws CacheLoaderException {
    // The following cast to V is to keep the compiler happy
    // but is erased at runtime and no actual cast happens.
    return (V) this.manager.read(helper.getRegion(), helper.getKey());
  }

  public void init(Properties props) {
    JDBCConfiguration config = new JDBCConfiguration(props);
    this.manager = new JDBCManager(config);
  };
}
