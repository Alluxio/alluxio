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

public class PrestoCacheScopeTest {

  @Test
  public void parent() {
    PrestoCacheScope schema = PrestoCacheScope.create("schema");
    PrestoCacheScope table = PrestoCacheScope.create("schema.table");
    PrestoCacheScope partition = PrestoCacheScope.create("schema.table.partition");
    assertNull(PrestoCacheScope.GLOBAL.parent());
    assertNull(schema.parent().parent());
    assertEquals(PrestoCacheScope.GLOBAL, schema.parent());
    assertEquals(schema, table.parent());
    assertEquals(table, partition.parent());
    assertEquals(PrestoCacheScope.create("schema.table.partition1").parent(),
        PrestoCacheScope.create("schema.table.partition2").parent());
    assertEquals(PrestoCacheScope.create("schema.table.part").parent(),
        PrestoCacheScope.create("schema.table.partition").parent());
    assertEquals(PrestoCacheScope.create("schema.table.part").parent().parent(),
        PrestoCacheScope.create("schema.table").parent());
  }

  @Test
  public void scopeHashCode() {
    assertEquals(PrestoCacheScope.GLOBAL.hashCode(), PrestoCacheScope.create(".").hashCode());
    assertEquals(PrestoCacheScope.create(".").hashCode(), PrestoCacheScope.create(".").hashCode());
    assertEquals(PrestoCacheScope.create("schema").hashCode(), PrestoCacheScope.create("schema").hashCode());
    assertEquals(PrestoCacheScope.create("schema.table").hashCode(), PrestoCacheScope.create("schema.table").hashCode());
    assertEquals(PrestoCacheScope.create("schema.table.partition").hashCode(),
        PrestoCacheScope.create("schema.table.partition").hashCode());
    assertEquals(PrestoCacheScope.create(".").hashCode(), PrestoCacheScope.create("schema").parent().hashCode());
    assertEquals(PrestoCacheScope.create("schema").hashCode(),
        PrestoCacheScope.create("schema.table").parent().hashCode());
    assertEquals(PrestoCacheScope.create("schema.table").hashCode(),
        PrestoCacheScope.create("schema.table.partition").parent().hashCode());
  }

  @Test
  public void scopeEquals() {
    assertEquals(PrestoCacheScope.GLOBAL, PrestoCacheScope.create("."));
    assertEquals(PrestoCacheScope.create("."), PrestoCacheScope.create("."));
    assertEquals(PrestoCacheScope.create("schema"), PrestoCacheScope.create("schema"));
    assertEquals(PrestoCacheScope.create("schema.table"), PrestoCacheScope.create("schema.table"));
    assertEquals(PrestoCacheScope.create("schema.table.partition"), PrestoCacheScope.create("schema.table.partition"));
    assertNotEquals(PrestoCacheScope.create("schema1"), PrestoCacheScope.create("schema2"));
    assertNotEquals(PrestoCacheScope.create("schema.table1"), PrestoCacheScope.create("schema.table2"));
    assertNotEquals(PrestoCacheScope.create("schema.Atable"), PrestoCacheScope.create("schema.Btable"));
    assertNotEquals(PrestoCacheScope.create("schema.table"), PrestoCacheScope.create("schema.table.partition"));
    assertNotEquals(PrestoCacheScope.create("schema.table.partition1"),
        PrestoCacheScope.create("schema.table.partition2"));
  }
}
