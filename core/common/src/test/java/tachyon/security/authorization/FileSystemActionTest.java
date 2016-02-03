/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security.authorization;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the {@link FileSystemAction} class.
 */
public final class FileSystemActionTest {

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
