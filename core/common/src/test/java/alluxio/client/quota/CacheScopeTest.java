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

package alluxio.client.quota;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Tests for CacheScope.
 */
public class CacheScopeTest {

  @Test
  public void parent() {
    CacheScope schema = CacheScope.create("schema");
    CacheScope table = CacheScope.create("schema.table");
    CacheScope partition = CacheScope.create("schema.table.partition");
    assertNull(CacheScope.GLOBAL.parent());
    assertNull(schema.parent().parent());
    assertEquals(CacheScope.GLOBAL, schema.parent());
    assertEquals(schema, table.parent());
    assertEquals(table, partition.parent());
    assertEquals(CacheScope.create("schema.table.partition1").parent(),
        CacheScope.create("schema.table.partition2").parent());
    assertEquals(CacheScope.create("schema.table.part").parent(),
        CacheScope.create("schema.table.partition").parent());
    assertEquals(CacheScope.create("schema.table.part").parent().parent(),
        CacheScope.create("schema.table").parent());
  }

  @Test
  public void scopeHashCode() {
    assertEquals(CacheScope.GLOBAL.hashCode(), CacheScope.create(".").hashCode());
    assertEquals(CacheScope.create(".").hashCode(), CacheScope.create(".").hashCode());
    assertEquals(CacheScope.create("schema").hashCode(), CacheScope.create("schema").hashCode());
    assertEquals(CacheScope.create("schema.table").hashCode(),
        CacheScope.create("schema.table").hashCode());
    assertEquals(CacheScope.create("schema.table.partition").hashCode(),
        CacheScope.create("schema.table.partition").hashCode());
    assertEquals(CacheScope.create(".").hashCode(),
        CacheScope.create("schema").parent().hashCode());
    assertEquals(CacheScope.create("schema").hashCode(),
        CacheScope.create("schema.table").parent().hashCode());
    assertEquals(CacheScope.create("schema.table").hashCode(),
        CacheScope.create("schema.table.partition").parent().hashCode());
  }

  @Test
  public void scopeEquals() {
    assertEquals(CacheScope.GLOBAL, CacheScope.create("."));
    assertEquals(CacheScope.create("."), CacheScope.create("."));
    assertEquals(CacheScope.create("schema"), CacheScope.create("schema"));
    assertEquals(CacheScope.create("schema.table"), CacheScope.create("schema.table"));
    assertEquals(CacheScope.create("schema.table.partition"),
        CacheScope.create("schema.table.partition"));
    assertNotEquals(CacheScope.create("schema1"), CacheScope.create("schema2"));
    assertNotEquals(CacheScope.create("schema.table1"), CacheScope.create("schema.table2"));
    assertNotEquals(CacheScope.create("schema.Atable"), CacheScope.create("schema.Btable"));
    assertNotEquals(CacheScope.create("schema.table"), CacheScope.create("schema.table.partition"));
    assertNotEquals(CacheScope.create("schema.table.partition1"),
        CacheScope.create("schema.table.partition2"));
  }
}
