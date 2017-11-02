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

import static com.googlecode.catchexception.CatchException.catchException;
import static com.googlecode.catchexception.CatchException.caughtException;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.apache.geode.cache.Operation;
import org.apache.geode.connectors.jdbc.internal.JDBCConfiguration;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(UnitTest.class)
public class JDBCConfigurationUnitTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testInvalidProperty() {
    Properties props = new Properties();
    props.setProperty("invalid", "");

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("unknown properties: [invalid]");
    new JDBCConfiguration(props);
  }

  @Test
  public void testMissingAllRequiredProperties() {
    Properties props = new Properties();
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("missing required properties: [url]");
    new JDBCConfiguration(props);
  }

  @Test
  public void testURLProperty() {
    Properties props = new Properties();
    props.setProperty("url", "myUrl");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getURL()).isEqualTo("myUrl");
  }

  @Test
  public void testDefaultUser() {
    Properties props = new Properties();
    props.setProperty("url", "");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getUser()).isNull();
  }

  @Test
  public void testDefaultPassword() {
    Properties props = new Properties();
    props.setProperty("url", "");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getPassword()).isNull();
  }

  @Test
  public void testUser() {
    Properties props = new Properties();
    props.setProperty("url", "");
    props.setProperty("user", "myUser");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getUser()).isEqualTo("myUser");
  }

  @Test
  public void testPassword() {
    Properties props = new Properties();
    props.setProperty("url", "");
    props.setProperty("password", "myPassword");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getPassword()).isEqualTo("myPassword");
  }

  @Test
  public void testDefaultValueClassName() {
    Properties props = new Properties();
    props.setProperty("url", "");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getValueClassName("foo")).isNull();
  }

  @Test
  public void testValueClassName() {
    Properties props = new Properties();
    props.setProperty("url", "");
    props.setProperty("valueClassName", "myPackage.myDomainClass");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getValueClassName("foo")).isEqualTo("myPackage.myDomainClass");
  }

  @Test(expected = IllegalArgumentException.class)
  public void verifyThatTwoClassNamesWithNoRegionNameThrows() {
    Properties props = new Properties();
    props.setProperty("url", "");
    props.setProperty("valueClassName", "myClass1, myClass2");
    new JDBCConfiguration(props);
  }

  @Test
  public void testValueClassNameWithRegionNames() {
    Properties props = new Properties();
    props.setProperty("url", "");
    props.setProperty("valueClassName", "reg1:cn1   , reg2:pack2.cn2,myPackage.myDomainClass");
    JDBCConfiguration config = new JDBCConfiguration(props);
    assertThat(config.getValueClassName("foo")).isEqualTo("myPackage.myDomainClass");
    assertThat(config.getValueClassName("reg1")).isEqualTo("cn1");
    assertThat(config.getValueClassName("reg2")).isEqualTo("pack2.cn2");
  }

}
