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

package alluxio.table.under.glue;

import static org.junit.Assert.assertEquals;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class GlueDatabaseTest {

  private static final String DB_NAME = "test";

  @Rule
  public ExpectedException mExpection = ExpectedException.none();

  private UdbContext mUdbContext;
  private UdbConfiguration mUdbConfiguration;

  @Before
  public void before() {
    Map<String, String> conf = ImmutableMap.of("aws.region", "us-east-1");
    mUdbConfiguration = new UdbConfiguration(conf);
    mUdbContext = new UdbContext(null, null, "glue", "null", DB_NAME, DB_NAME);
  }

  @Test
  public void create() {
    assertEquals(DB_NAME, GlueDatabase.create(mUdbContext, mUdbConfiguration).getName());
  }

  @Test
  public void createEmptyName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
        new UdbContext(null, null, "glue", null, "", DB_NAME);
    assertEquals(DB_NAME,
        GlueDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }

  @Test
  public void createNullName() {
    mExpection.expect(IllegalArgumentException.class);
    UdbContext udbContext =
        new UdbContext(null, null, "glue", null, null, DB_NAME);
    assertEquals(DB_NAME,
        GlueDatabase.create(udbContext, new UdbConfiguration(ImmutableMap.of())).getName());
  }
}
