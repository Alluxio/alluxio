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

package tachyon.client.file;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link TachyonFile}.
 */
public final class TachyonFileTest {

  /**
   * Test for the {@link TachyonFile#equals(Object)} method.
   */
  @Test
  public void equalsTest() {
    TachyonFile file1 = new TachyonFile(1);
    TachyonFile file2 = new TachyonFile(1);
    TachyonFile file3 = new TachyonFile(2);

    Assert.assertNotEquals(file1, null);
    Assert.assertEquals(file1, file1);
    Assert.assertEquals(file1, file2);
    Assert.assertEquals(file2, file1);
    Assert.assertNotEquals(file1, file3);
  }

  /**
   * Test for the {@link TachyonFile#hashCode()} method.
   */
  @Test
  public void hashCodeTest() {
    TachyonFile file1 = new TachyonFile(1);
    TachyonFile file2 = new TachyonFile(1);
    TachyonFile file3 = new TachyonFile(2);

    Assert.assertEquals(file1.hashCode(), file1.hashCode());
    Assert.assertEquals(file1.hashCode(), file2.hashCode());
    Assert.assertNotEquals(file1.hashCode(), file3.hashCode());
  }

  /**
   * Test for the {@link TachyonFile#getFileId()} method.
   */
  @Test
  public void getFileIdTest() {
    Assert.assertEquals(100, new TachyonFile(100).getFileId());
  }

  /**
   * Test for the {@link TachyonFile#toString()} method.
   */
  @Test
  public void toStringTest() {
    Assert.assertEquals("TachyonFile(100)", new TachyonFile(100).toString());
  }
}
