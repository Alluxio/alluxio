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

public class ScopeTest {

  @Test
  public void getParent() {
    Scope schema = Scope.create("schema");
    Scope table = Scope.create("schema.table");
    Scope partition = Scope.create("schema.table.partition");
    assertNull(Scope.GLOBAL.parent());
    assertEquals(Scope.GLOBAL, schema.parent());
    assertEquals(schema, table.parent());
    assertEquals(table, partition.parent());
  }

  @Test
  public void scopeEquals() {
    assertEquals(Scope.GLOBAL, Scope.create("."));
    assertEquals(Scope.create("."), Scope.create("."));
    assertEquals(Scope.create("schema.table"), Scope.create("schema.table"));
    assertEquals(Scope.create("schema.table.partition"), Scope.create("schema.table.partition"));
    assertNotEquals(Scope.create("schema1"), Scope.create("schema2"));
    assertNotEquals(Scope.create("schema.table1"), Scope.create("schema.table2"));
    assertNotEquals(Scope.create("schema.table"), Scope.create("schema.table.partition"));
    assertNotEquals(Scope.create("schema.table.partition1"),
        Scope.create("schema.table.partition2"));
  }
}
