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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link Mode.Bits} class.
 */
public final class ModeBitsTest {

  /**
   * Tests the {@link Mode.Bits#toString()} method.
   */
  @Test
  public void getSymbol() {
    assertEquals("---", Mode.Bits.NONE.toString());
    assertEquals("r--", Mode.Bits.READ.toString());
    assertEquals("-w-", Mode.Bits.WRITE.toString());
    assertEquals("--x", Mode.Bits.EXECUTE.toString());
    assertEquals("rw-", Mode.Bits.READ_WRITE.toString());
    assertEquals("r-x", Mode.Bits.READ_EXECUTE.toString());
    assertEquals("-wx", Mode.Bits.WRITE_EXECUTE.toString());
    assertEquals("rwx", Mode.Bits.ALL.toString());
  }

  /**
   * Tests the {@link Mode.Bits#imply(Mode.Bits)} method.
   */
  @Test
  public void implies() {
    assertTrue(Mode.Bits.ALL.imply(Mode.Bits.READ));
    assertTrue(Mode.Bits.ALL.imply(Mode.Bits.WRITE));
    assertTrue(Mode.Bits.ALL.imply(Mode.Bits.EXECUTE));
    assertTrue(Mode.Bits.ALL.imply(Mode.Bits.READ_EXECUTE));
    assertTrue(Mode.Bits.ALL.imply(Mode.Bits.WRITE_EXECUTE));
    assertTrue(Mode.Bits.ALL.imply(Mode.Bits.ALL));

    assertTrue(Mode.Bits.READ_EXECUTE.imply(Mode.Bits.READ));
    assertTrue(Mode.Bits.READ_EXECUTE.imply(Mode.Bits.EXECUTE));
    assertFalse(Mode.Bits.READ_EXECUTE.imply(Mode.Bits.WRITE));

    assertTrue(Mode.Bits.WRITE_EXECUTE.imply(Mode.Bits.WRITE));
    assertTrue(Mode.Bits.WRITE_EXECUTE.imply(Mode.Bits.EXECUTE));
    assertFalse(Mode.Bits.WRITE_EXECUTE.imply(Mode.Bits.READ));

    assertTrue(Mode.Bits.READ_WRITE.imply(Mode.Bits.WRITE));
    assertTrue(Mode.Bits.READ_WRITE.imply(Mode.Bits.READ));
    assertFalse(Mode.Bits.READ_WRITE.imply(Mode.Bits.EXECUTE));
  }

  /**
   * Tests the {@link Mode.Bits#not()} method.
   */
  @Test
  public void notOperation() {
    assertEquals(Mode.Bits.WRITE, Mode.Bits.READ_EXECUTE.not());
    assertEquals(Mode.Bits.READ, Mode.Bits.WRITE_EXECUTE.not());
    assertEquals(Mode.Bits.EXECUTE, Mode.Bits.READ_WRITE.not());
  }

  /**
   * Tests the {@link Mode.Bits#or(Mode.Bits)} method.
   */
  @Test
  public void orOperation() {
    assertEquals(Mode.Bits.WRITE_EXECUTE, Mode.Bits.WRITE.or(Mode.Bits.EXECUTE));
    assertEquals(Mode.Bits.READ_EXECUTE, Mode.Bits.READ.or(Mode.Bits.EXECUTE));
    assertEquals(Mode.Bits.READ_WRITE, Mode.Bits.WRITE.or(Mode.Bits.READ));
  }

  /**
   * Tests the {@link Mode.Bits#and(Mode.Bits)} method.
   */
  @Test
  public void andOperation() {
    assertEquals(Mode.Bits.NONE, Mode.Bits.READ.and(Mode.Bits.WRITE));
    assertEquals(Mode.Bits.READ, Mode.Bits.READ_EXECUTE.and(Mode.Bits.READ));
    assertEquals(Mode.Bits.WRITE, Mode.Bits.READ_WRITE.and(Mode.Bits.WRITE));
  }

  /**
   * Tests {@link Mode.Bits#toAclActionSet()}.
   */
  @Test
  public void toAclActions() {
    for (Mode.Bits bits : Mode.Bits.values()) {
      Assert.assertEquals(bits, new AclActions(bits.toAclActionSet()).toModeBits());
    }
  }
}
