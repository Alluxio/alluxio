/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
 * Tests the {@link FileSystemAction} class.
 */
public final class FileSystemActionTest {

  /**
   * Tests the {@link FileSystemAction#getSymbol()} method.
   */
  @Test
  public void getSymbolTest() {
    Assert.assertEquals("---", FileSystemAction.NONE.getSymbol());
    Assert.assertEquals("r--", FileSystemAction.READ.getSymbol());
    Assert.assertEquals("-w-", FileSystemAction.WRITE.getSymbol());
    Assert.assertEquals("--x", FileSystemAction.EXECUTE.getSymbol());
    Assert.assertEquals("rw-", FileSystemAction.READ_WRITE.getSymbol());
    Assert.assertEquals("r-x", FileSystemAction.READ_EXECUTE.getSymbol());
    Assert.assertEquals("-wx", FileSystemAction.WRITE_EXECUTE.getSymbol());
    Assert.assertEquals("rwx", FileSystemAction.ALL.getSymbol());
  }

  /**
   * Tests the {@link FileSystemAction#imply(FileSystemAction)} method.
   */
  @Test
  public void impliesTest() {
    Assert.assertTrue(FileSystemAction.ALL.imply(FileSystemAction.READ));
    Assert.assertTrue(FileSystemAction.ALL.imply(FileSystemAction.WRITE));
    Assert.assertTrue(FileSystemAction.ALL.imply(FileSystemAction.EXECUTE));
    Assert.assertTrue(FileSystemAction.ALL.imply(FileSystemAction.READ_EXECUTE));
    Assert.assertTrue(FileSystemAction.ALL.imply(FileSystemAction.WRITE_EXECUTE));
    Assert.assertTrue(FileSystemAction.ALL.imply(FileSystemAction.ALL));

    Assert.assertTrue(FileSystemAction.READ_EXECUTE.imply(FileSystemAction.READ));
    Assert.assertTrue(FileSystemAction.READ_EXECUTE.imply(FileSystemAction.EXECUTE));
    Assert.assertFalse(FileSystemAction.READ_EXECUTE.imply(FileSystemAction.WRITE));

    Assert.assertTrue(FileSystemAction.WRITE_EXECUTE.imply(FileSystemAction.WRITE));
    Assert.assertTrue(FileSystemAction.WRITE_EXECUTE.imply(FileSystemAction.EXECUTE));
    Assert.assertFalse(FileSystemAction.WRITE_EXECUTE.imply(FileSystemAction.READ));

    Assert.assertTrue(FileSystemAction.READ_WRITE.imply(FileSystemAction.WRITE));
    Assert.assertTrue(FileSystemAction.READ_WRITE.imply(FileSystemAction.READ));
    Assert.assertFalse(FileSystemAction.READ_WRITE.imply(FileSystemAction.EXECUTE));
  }

  /**
   * Tests the {@link FileSystemAction#not()} method.
   */
  @Test
  public void notOperationTest() {
    Assert.assertEquals(FileSystemAction.WRITE, FileSystemAction.READ_EXECUTE.not());
    Assert.assertEquals(FileSystemAction.READ, FileSystemAction.WRITE_EXECUTE.not());
    Assert.assertEquals(FileSystemAction.EXECUTE, FileSystemAction.READ_WRITE.not());
  }

  /**
   * Tests the {@link FileSystemAction#or(FileSystemAction)} method.
   */
  @Test
  public void orOperationTest() {
    Assert.assertEquals(FileSystemAction.WRITE_EXECUTE,
        FileSystemAction.WRITE.or(FileSystemAction.EXECUTE));
    Assert.assertEquals(FileSystemAction.READ_EXECUTE,
        FileSystemAction.READ.or(FileSystemAction.EXECUTE));
    Assert.assertEquals(FileSystemAction.READ_WRITE,
        FileSystemAction.WRITE.or(FileSystemAction.READ));
  }

  /**
   * Tests the {@link FileSystemAction#and(FileSystemAction)} method.
   */
  @Test
  public void andOperationTest() {
    Assert.assertEquals(FileSystemAction.NONE, FileSystemAction.READ.and(FileSystemAction.WRITE));
    Assert.assertEquals(FileSystemAction.READ,
        FileSystemAction.READ_EXECUTE.and(FileSystemAction.READ));
    Assert.assertEquals(FileSystemAction.WRITE,
        FileSystemAction.READ_WRITE.and(FileSystemAction.WRITE));
  }
}
