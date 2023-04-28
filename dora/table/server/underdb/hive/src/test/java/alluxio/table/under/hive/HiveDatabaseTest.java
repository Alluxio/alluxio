/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.table.under.hive;

import static org.junit.Assert.assertEquals;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

public class HiveDatabaseTest {

  private static final String DB_NAME = "test";
  private static final Map<String, String> CONF = new HashMap<>();

  @Rule
  public ExpectedException mExpection = ExpectedException.none();

  private UdbContext mUdbContext;
  private UdbConfiguration mUdbConf;

  @Before
  public void before() {
    mUdbContext =
        new UdbContext(null, null, "hive", "thrift://not_running:9083", DB_NAME, DB_NAME);
    mUdbConf = new UdbConfiguration(CONF);
  }

  @Test
  public void create() {
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext, mUdbConf).getName());
  }

  @Test
  public void createEmptyName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
        new UdbContext(null, null, "hive", "thrift://not_running:9083", "", DB_NAME);
    assertEquals(DB_NAME,
        HiveDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createNullName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
        new UdbContext(null, null, "hive", "thrift://not_running:9083", null, DB_NAME);
    assertEquals(DB_NAME,
        HiveDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createEmptyConnectionUri() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext = new UdbContext(null, null, "hive", "", DB_NAME, DB_NAME);
    assertEquals(DB_NAME, HiveDatabase.create(udbContext,
        new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createNullConnectionUri() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext = new UdbContext(null, null, "hive", null, DB_NAME, DB_NAME);
    assertEquals(DB_NAME, HiveDatabase.create(udbContext,
        new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createWithProps() {
    Map<String, String> props = ImmutableMap.of(
        "prop1", "value1",
        "prop2", "value2"
    );
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext, new UdbConfiguration(props)).getName());
  }
}
