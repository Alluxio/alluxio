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

package alluxio.security.authorization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests {@link AclActions} class.
 */
public class AclActionsTest {
  /**
   * Tests constructor contract.
   */
  @Test
  public void constructor() {
    AclActions actions = new AclActions();
    assertTrue(actions.getActions().isEmpty());

    AclActions copiedActions = new AclActions(actions);
    copiedActions.add(AclAction.READ);
    assertEquals(1, copiedActions.getActions().size());
    assertEquals(0, actions.getActions().size());
  }

  /**
   * Tests {@link AclActions#toModeBits()}.
   */
  @Test
  public void toModeBits() {
    AclActions actions = new AclActions();
    assertEquals(Mode.Bits.NONE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    assertEquals(Mode.Bits.READ, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.WRITE);
    assertEquals(Mode.Bits.WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.EXECUTE);
    assertEquals(Mode.Bits.EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    actions.add(AclAction.WRITE);
    assertEquals(Mode.Bits.READ_WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    actions.add(AclAction.EXECUTE);
    assertEquals(Mode.Bits.READ_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.WRITE);
    actions.add(AclAction.EXECUTE);
    assertEquals(Mode.Bits.WRITE_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    actions.add(AclAction.WRITE);
    actions.add(AclAction.EXECUTE);
    assertEquals(Mode.Bits.ALL, actions.toModeBits());
  }

  /**
   * Tests {@link AclActions#updateByModeBits(Mode.Bits)}.
   */
  @Test
  public void updateByModeBits() {
    AclActions actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.NONE);
    assertEquals(Mode.Bits.NONE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.READ);
    assertEquals(Mode.Bits.READ, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.WRITE);
    assertEquals(Mode.Bits.WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.EXECUTE);
    assertEquals(Mode.Bits.EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.READ_WRITE);
    assertEquals(Mode.Bits.READ_WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.READ_EXECUTE);
    assertEquals(Mode.Bits.READ_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.WRITE_EXECUTE);
    assertEquals(Mode.Bits.WRITE_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.ALL);
    assertEquals(Mode.Bits.ALL, actions.toModeBits());
  }

  /**
   * Tests {@link AclActions#contains(AclAction)}.
   */
  @Test
  public void contains() {
    AclActions actions = new AclActions();
    assertFalse(actions.contains(AclAction.READ));
    assertFalse(actions.contains(AclAction.WRITE));
    assertFalse(actions.contains(AclAction.EXECUTE));

    actions.add(AclAction.READ);
    assertTrue(actions.contains(AclAction.READ));
    actions.add(AclAction.WRITE);
    assertTrue(actions.contains(AclAction.WRITE));
    actions.add(AclAction.EXECUTE);
    assertTrue(actions.contains(AclAction.EXECUTE));
  }

  /**
   * Tests {@link AclActions#merge(AclActions)}.
   */
  @Test
  public void merge() {
    AclActions actions = new AclActions();
    assertEquals(Mode.Bits.NONE, actions.toModeBits());

    // Merge empty actions.
    actions.merge(new AclActions());
    assertEquals(Mode.Bits.NONE, actions.toModeBits());

    // Merge read and write actions.
    AclActions readWrite = new AclActions();
    readWrite.add(AclAction.READ);
    readWrite.add(AclAction.WRITE);
    actions.merge(readWrite);
    assertEquals(Mode.Bits.READ_WRITE, actions.toModeBits());

    // Merge execute action.
    AclActions execute = new AclActions();
    execute.add(AclAction.EXECUTE);
    actions.merge(execute);
    assertEquals(Mode.Bits.ALL, actions.toModeBits());
  }
}
