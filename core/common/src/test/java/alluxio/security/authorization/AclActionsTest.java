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

import org.junit.Assert;
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
    Assert.assertTrue(actions.getActions().isEmpty());

    AclActions copiedActions = new AclActions(actions);
    copiedActions.add(AclAction.READ);
    Assert.assertEquals(1, copiedActions.getActions().size());
    Assert.assertEquals(0, actions.getActions().size());
  }

  /**
   * Tests {@link AclActions#toModeBits()}.
   */
  @Test
  public void toModeBits() {
    AclActions actions = new AclActions();
    Assert.assertEquals(Mode.Bits.NONE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    Assert.assertEquals(Mode.Bits.READ, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.WRITE);
    Assert.assertEquals(Mode.Bits.WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.EXECUTE);
    Assert.assertEquals(Mode.Bits.EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    actions.add(AclAction.WRITE);
    Assert.assertEquals(Mode.Bits.READ_WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    actions.add(AclAction.EXECUTE);
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.WRITE);
    actions.add(AclAction.EXECUTE);
    Assert.assertEquals(Mode.Bits.WRITE_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.add(AclAction.READ);
    actions.add(AclAction.WRITE);
    actions.add(AclAction.EXECUTE);
    Assert.assertEquals(Mode.Bits.ALL, actions.toModeBits());
  }

  /**
   * Tests {@link AclActions#updateByModeBits(Mode.Bits)}.
   */
  @Test
  public void updateByModeBits() {
    AclActions actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.NONE);
    Assert.assertEquals(Mode.Bits.NONE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.READ);
    Assert.assertEquals(Mode.Bits.READ, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.WRITE);
    Assert.assertEquals(Mode.Bits.WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.EXECUTE);
    Assert.assertEquals(Mode.Bits.EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.READ_WRITE);
    Assert.assertEquals(Mode.Bits.READ_WRITE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.READ_EXECUTE);
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.WRITE_EXECUTE);
    Assert.assertEquals(Mode.Bits.WRITE_EXECUTE, actions.toModeBits());

    actions = new AclActions();
    actions.updateByModeBits(Mode.Bits.ALL);
    Assert.assertEquals(Mode.Bits.ALL, actions.toModeBits());
  }

  /**
   * Tests {@link AclActions#contains(AclAction)}.
   */
  @Test
  public void contains() {
    AclActions actions = new AclActions();
    Assert.assertFalse(actions.contains(AclAction.READ));
    Assert.assertFalse(actions.contains(AclAction.WRITE));
    Assert.assertFalse(actions.contains(AclAction.EXECUTE));

    actions.add(AclAction.READ);
    Assert.assertTrue(actions.contains(AclAction.READ));
    actions.add(AclAction.WRITE);
    Assert.assertTrue(actions.contains(AclAction.WRITE));
    actions.add(AclAction.EXECUTE);
    Assert.assertTrue(actions.contains(AclAction.EXECUTE));
  }

  /**
   * Tests {@link AclActions#merge(AclActions)}.
   */
  @Test
  public void merge() {
    AclActions actions = new AclActions();
    Assert.assertEquals(Mode.Bits.NONE, actions.toModeBits());

    // Merge empty actions.
    actions.merge(new AclActions());
    Assert.assertEquals(Mode.Bits.NONE, actions.toModeBits());

    // Merge read and write actions.
    AclActions readWrite = new AclActions();
    readWrite.add(AclAction.READ);
    readWrite.add(AclAction.WRITE);
    actions.merge(readWrite);
    Assert.assertEquals(Mode.Bits.READ_WRITE, actions.toModeBits());

    // Merge execute action.
    AclActions execute = new AclActions();
    execute.add(AclAction.EXECUTE);
    actions.merge(execute);
    Assert.assertEquals(Mode.Bits.ALL, actions.toModeBits());
  }
}
