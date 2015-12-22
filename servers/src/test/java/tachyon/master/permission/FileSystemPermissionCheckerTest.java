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

package tachyon.master.permission;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * Unit tests for {@link FileSystemPermissionChecker}
 */
public class FileSystemPermissionCheckerTest {

  @Before
  public void before() throws Exception {
    Whitebox.setInternalState(FileSystemPermissionChecker.class, "sPermissionCheckEnabled", false);
    Whitebox.setInternalState(FileSystemPermissionChecker.class, "sFileSystemOwner",
        (String) null);
    Whitebox.setInternalState(FileSystemPermissionChecker.class, "sFileSystemSuperGroup",
        (String) null);
  }

  @Test
  public void testInitialization() throws Exception {
    FileSystemPermissionChecker.initializeFileSystem(true, "user1", "group1");
    Assert.assertEquals(true, Whitebox.getInternalState(FileSystemPermissionChecker.class,
        "sPermissionCheckEnabled"));
    Assert.assertEquals("user1", Whitebox.getInternalState(FileSystemPermissionChecker.class,
        "sFileSystemOwner"));
    Assert.assertEquals("group1", Whitebox.getInternalState(FileSystemPermissionChecker.class,
        "sFileSystemSuperGroup"));
  }

  // TODO(dong): add tests for permission check methods
}
