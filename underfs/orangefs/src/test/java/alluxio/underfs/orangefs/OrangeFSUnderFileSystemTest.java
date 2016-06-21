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

package alluxio.underfs.orangefs;

import alluxio.AlluxioURI;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link OrangeFSUnderFileSystem} that do not require an
 * OrangeFS backend.
 */
public class OrangeFSUnderFileSystemTest {
  private OrangeFSUnderFileSystem mMockOrangeFSUnderFileSystem;

  /**
   * Sets up the mock before a test runs.
   */
  @Before
  public  final void before() {
    mMockOrangeFSUnderFileSystem = PowerMockito.mock(OrangeFSUnderFileSystem.class);
    Whitebox.setInternalState(mMockOrangeFSUnderFileSystem, "mOfsMount", "/mnt/orangefs");
  }

  /**
   * Tests the {@link OrangeFSUnderFileSystem#getOFSPath(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void getOFSPathTest1() throws Exception {
    String path1 = "ofs://localhost:3334/test";
    String path2 = "ofs://localhost:3334/test/input";
    String path3 = "ofs://localhost:3334/test/output";
    String result1 =
        Whitebox.invokeMethod(mMockOrangeFSUnderFileSystem, "getOFSPath", path1);
    String result2 =
        Whitebox.invokeMethod(mMockOrangeFSUnderFileSystem, "getOFSPath", path2);
    String result3 =
        Whitebox.invokeMethod(mMockOrangeFSUnderFileSystem, "getOFSPath", path3);

    Assert.assertEquals("/mnt/orangefs/test", result1);
    Assert.assertEquals("/mnt/orangefs/test/input", result2);
    Assert.assertEquals("/mnt/orangefs/test/output", result3);
  }

  /**
   * Tests the {@link OrangeFSUnderFileSystem#getOFSPath(AlluxioURI)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void getOFSPathTest2() throws Exception {
    AlluxioURI uri1 = new AlluxioURI("ofs://localhost:3334/test");
    AlluxioURI uri2 = new AlluxioURI("ofs://localhost:3334/test/input");
    AlluxioURI uri3 = new AlluxioURI("ofs://localhost:3334/test/output");
    String result1 =
        Whitebox.invokeMethod(mMockOrangeFSUnderFileSystem, "getOFSPath", uri1);
    String result2 =
        Whitebox.invokeMethod(mMockOrangeFSUnderFileSystem, "getOFSPath", uri2);
    String result3 =
        Whitebox.invokeMethod(mMockOrangeFSUnderFileSystem, "getOFSPath", uri3);

    Assert.assertEquals("/mnt/orangefs/test", result1);
    Assert.assertEquals("/mnt/orangefs/test/input", result2);
    Assert.assertEquals("/mnt/orangefs/test/output", result3);
  }
}
