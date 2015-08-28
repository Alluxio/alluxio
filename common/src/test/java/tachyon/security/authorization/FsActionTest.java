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

public class FsActionTest {

  @Test
  public void impliesTest() throws Exception {
    Assert.assertTrue(FsAction.ALL.implies(FsAction.READ));
    Assert.assertTrue(FsAction.ALL.implies(FsAction.WRITE));
    Assert.assertTrue(FsAction.ALL.implies(FsAction.EXECUTE));
    Assert.assertTrue(FsAction.ALL.implies(FsAction.READ_EXECUTE));
    Assert.assertTrue(FsAction.ALL.implies(FsAction.WRITE_EXECUTE));
    Assert.assertTrue(FsAction.ALL.implies(FsAction.ALL));

    Assert.assertTrue(FsAction.READ_EXECUTE.implies(FsAction.READ));
    Assert.assertTrue(FsAction.READ_EXECUTE.implies(FsAction.EXECUTE));
    Assert.assertFalse(FsAction.READ_EXECUTE.implies(FsAction.WRITE));

    Assert.assertTrue(FsAction.WRITE_EXECUTE.implies(FsAction.WRITE));
    Assert.assertTrue(FsAction.WRITE_EXECUTE.implies(FsAction.EXECUTE));
    Assert.assertFalse(FsAction.WRITE_EXECUTE.implies(FsAction.READ));

    Assert.assertTrue(FsAction.READ_WRITE.implies(FsAction.WRITE));
    Assert.assertTrue(FsAction.READ_WRITE.implies(FsAction.READ));
    Assert.assertFalse(FsAction.READ_WRITE.implies(FsAction.EXECUTE));
  }

  @Test
  public void notOperationTest() throws Exception {
    Assert.assertEquals(FsAction.WRITE, FsAction.READ_EXECUTE.not());
    Assert.assertEquals(FsAction.READ, FsAction.WRITE_EXECUTE.not());
    Assert.assertEquals(FsAction.EXECUTE, FsAction.READ_WRITE.not());
  }

  @Test
  public void orOperationTest() throws Exception {
    Assert.assertEquals(FsAction.WRITE_EXECUTE, FsAction.WRITE.or(FsAction.EXECUTE));
    Assert.assertEquals(FsAction.READ_EXECUTE, FsAction.READ.or(FsAction.EXECUTE));
    Assert.assertEquals(FsAction.READ_WRITE, FsAction.WRITE.or(FsAction.READ));
  }

  @Test
  public void andOperationTest() throws Exception {
    Assert.assertEquals(FsAction.NONE, FsAction.READ.and(FsAction.WRITE));
    Assert.assertEquals(FsAction.READ, FsAction.READ_EXECUTE.and(FsAction.READ));
    Assert.assertEquals(FsAction.WRITE, FsAction.READ_WRITE.and(FsAction.WRITE));
  }
}
