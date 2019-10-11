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
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class HiveDatabaseTest {

  private static final String DB_NAME = "test";
  private static final Map<String, String> CONF = new HashMap<>();

  static {
    CONF.put(Property.DATABASE_NAME.getName(), DB_NAME);
    CONF.put(Property.HIVE_METASTORE_URIS.getName(), "thrift://not_running:9083");
  }

  @Rule
  public ExpectedException mExpection = ExpectedException.none();

  private UdbContext mUdbContext;
  private UdbConfiguration mUdbConf;
  private HiveDatabase mHiveDb;

  @Before
  public void before() {
    mUdbContext = Mockito.mock(UdbContext.class);
    mUdbConf = new UdbConfiguration(CONF);
    mHiveDb = HiveDatabase.create(mUdbContext, mUdbConf);
  }

  @Test
  public void testCreateWithName() {
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext, mUdbConf).getName());
  }

  @Test
  public void testCreateWithNoConfig() {
    mExpection.expect(IllegalArgumentException.class);
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext,
        new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void testCreateWithNoName() {
    mExpection.expect(IllegalArgumentException.class);
    Map<String, String> props = ImmutableMap.of(
        Property.HIVE_METASTORE_URIS.getName(), "thrift:///"
    );
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext,
        new UdbConfiguration(props)).getName());
  }

  @Test
  public void testCreateWithProps() {
    Map<String, String> props = ImmutableMap.of(
        Property.HIVE_METASTORE_URIS.getName(), "thrift:///",
        Property.DATABASE_NAME.getName(), DB_NAME
    );
    assertEquals(DB_NAME, HiveDatabase.create(mUdbContext, new UdbConfiguration(props)).getName());
  }
}
