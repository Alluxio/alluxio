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
 * Tests the {@link Mode.Bits} class.
 */
public final class ModeBitsTest {

  /**
   * Tests the {@link Mode.Bits#toString()} method.
   */
  @Test
  public void getSymbol() {
    Assert.assertEquals("---", Mode.Bits.NONE.toString());
    Assert.assertEquals("r--", Mode.Bits.READ.toString());
    Assert.assertEquals("-w-", Mode.Bits.WRITE.toString());
    Assert.assertEquals("--x", Mode.Bits.EXECUTE.toString());
    Assert.assertEquals("rw-", Mode.Bits.READ_WRITE.toString());
    Assert.assertEquals("r-x", Mode.Bits.READ_EXECUTE.toString());
    Assert.assertEquals("-wx", Mode.Bits.WRITE_EXECUTE.toString());
    Assert.assertEquals("rwx", Mode.Bits.ALL.toString());
  }

  /**
   * Tests the {@link Mode.Bits#imply(Mode.Bits)} method.
   */
  @Test
  public void implies() {
    Assert.assertTrue(Mode.Bits.ALL.imply(Mode.Bits.READ));
    Assert.assertTrue(Mode.Bits.ALL.imply(Mode.Bits.WRITE));
    Assert.assertTrue(Mode.Bits.ALL.imply(Mode.Bits.EXECUTE));
    Assert.assertTrue(Mode.Bits.ALL.imply(Mode.Bits.READ_EXECUTE));
    Assert.assertTrue(Mode.Bits.ALL.imply(Mode.Bits.WRITE_EXECUTE));
    Assert.assertTrue(Mode.Bits.ALL.imply(Mode.Bits.ALL));

    Assert.assertTrue(Mode.Bits.READ_EXECUTE.imply(Mode.Bits.READ));
    Assert.assertTrue(Mode.Bits.READ_EXECUTE.imply(Mode.Bits.EXECUTE));
    Assert.assertFalse(Mode.Bits.READ_EXECUTE.imply(Mode.Bits.WRITE));

    Assert.assertTrue(Mode.Bits.WRITE_EXECUTE.imply(Mode.Bits.WRITE));
    Assert.assertTrue(Mode.Bits.WRITE_EXECUTE.imply(Mode.Bits.EXECUTE));
    Assert.assertFalse(Mode.Bits.WRITE_EXECUTE.imply(Mode.Bits.READ));

    Assert.assertTrue(Mode.Bits.READ_WRITE.imply(Mode.Bits.WRITE));
    Assert.assertTrue(Mode.Bits.READ_WRITE.imply(Mode.Bits.READ));
    Assert.assertFalse(Mode.Bits.READ_WRITE.imply(Mode.Bits.EXECUTE));
  }

  /**
   * Tests the {@link Mode.Bits#not()} method.
   */
  @Test
  public void notOperation() {
    Assert.assertEquals(Mode.Bits.WRITE, Mode.Bits.READ_EXECUTE.not());
    Assert.assertEquals(Mode.Bits.READ, Mode.Bits.WRITE_EXECUTE.not());
    Assert.assertEquals(Mode.Bits.EXECUTE, Mode.Bits.READ_WRITE.not());
  }

  /**
   * Tests the {@link Mode.Bits#or(Mode.Bits)} method.
   */
  @Test
  public void orOperation() {
    Assert.assertEquals(Mode.Bits.WRITE_EXECUTE, Mode.Bits.WRITE.or(Mode.Bits.EXECUTE));
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, Mode.Bits.READ.or(Mode.Bits.EXECUTE));
    Assert.assertEquals(Mode.Bits.READ_WRITE, Mode.Bits.WRITE.or(Mode.Bits.READ));
  }

  /**
   * Tests the {@link Mode.Bits#and(Mode.Bits)} method.
   */
  @Test
  public void andOperation() {
    Assert.assertEquals(Mode.Bits.NONE, Mode.Bits.READ.and(Mode.Bits.WRITE));
    Assert.assertEquals(Mode.Bits.READ, Mode.Bits.READ_EXECUTE.and(Mode.Bits.READ));
    Assert.assertEquals(Mode.Bits.WRITE, Mode.Bits.READ_WRITE.and(Mode.Bits.WRITE));
  }
}
