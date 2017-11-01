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

import java.util.Properties;

import org.apache.geode.CopyHelper;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.connectors.jdbc.internal.JDBCConfiguration;
import org.apache.geode.connectors.jdbc.internal.JDBCManager;
import org.apache.geode.pdx.PdxInstance;

/*
 * This class provides synchronous write through to a data source using JDBC.
 *
 * @since Geode 1.4
 */
public class JDBCSynchronousWriter<K, V> implements CacheWriter<K, V> {
  private JDBCManager manager;

  @Override
  public void init(Properties props) {
    JDBCConfiguration config = new JDBCConfiguration(props);
    this.manager = new JDBCManager(config);
  }

  @Override
  public void close() {}

  private PdxInstance getPdxNewValue(EntryEvent<K, V> event) {
    // TODO: have a better API that lets you do this
    DefaultQuery.setPdxReadSerialized(true);
    try {
      Object v = event.getNewValue();
      if (!(v instanceof PdxInstance)) {
        SerializedCacheValue<V> sv = event.getSerializedNewValue();
        if (sv != null) {
          v = sv.getDeserializedValue();
        } else {
          v = CopyHelper.copy(v);
        }
      }
      return (PdxInstance) v;
    } finally {
      DefaultQuery.setPdxReadSerialized(false);
    }
  }

  @Override
  public void beforeUpdate(EntryEvent<K, V> event) throws CacheWriterException {
    this.manager.write(event.getRegion(), event.getOperation(), event.getKey(),
        getPdxNewValue(event));
  }

  @Override
  public void beforeCreate(EntryEvent<K, V> event) throws CacheWriterException {
    this.manager.write(event.getRegion(), event.getOperation(), event.getKey(),
        getPdxNewValue(event));
  }

  @Override
  public void beforeDestroy(EntryEvent<K, V> event) throws CacheWriterException {
    this.manager.write(event.getRegion(), event.getOperation(), event.getKey(),
        getPdxNewValue(event));
  }

  @Override
  public void beforeRegionDestroy(RegionEvent<K, V> event) throws CacheWriterException {
    // this event is not sent to JDBC
  }

  @Override
  public void beforeRegionClear(RegionEvent<K, V> event) throws CacheWriterException {
    // this event is not sent to JDBC
  }

}
