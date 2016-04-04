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

package alluxio.underfs.swift;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link SwiftUnderFileSystem} that do not require a
 * Swift backend.
 */
public class SwiftUnderFileSystemTest {
  private SwiftUnderFileSystem mMockSwiftUnderFileSystem;
  private String mMockContainerName = "test-container";
  private String mMockContainerPrefix = "swift://" + mMockContainerName + "/";

  /**
   * Sets up the mock before a test runs.
   */
  @Before
  public  final void before() {
    mMockSwiftUnderFileSystem = PowerMockito.mock(SwiftUnderFileSystem.class);
    Whitebox.setInternalState(mMockSwiftUnderFileSystem, "mContainerName", mMockContainerName);
    Whitebox.setInternalState(mMockSwiftUnderFileSystem, "mContainerPrefix", mMockContainerPrefix);
  }

  /**
   * Tests the {@link SwiftUnderFileSystem#makeQualifiedPath(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void makeQualifiedPathTest() throws Exception {
    String input1 = "a/b";
    String input2 = "/a/b";
    String input3 = "a/b/";
    String input4 = "/a/b/";
    String result1 = Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "makeQualifiedPath", input1);
    String result2 = Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "makeQualifiedPath", input2);
    String result3 = Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "makeQualifiedPath", input3);
    String result4 = Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "makeQualifiedPath", input4);

    Assert.assertEquals(result1, "a/b/");
    Assert.assertEquals(result2, "a/b/");
    Assert.assertEquals(result3, "a/b/");
    Assert.assertEquals(result4, "a/b/");
  }

  /**
   * Tests the {@link SwiftUnderFileSystem#stripFolderSuffixIfPresent(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void stripFolderSuffixIfPresentTest() throws Exception {
    String input1 = mMockContainerPrefix;
    String input2 = mMockContainerPrefix + "dir/file";
    String input3 = mMockContainerPrefix + "dir_$folder$";
    String result1 =
        Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "stripFolderSuffixIfPresent", input1);
    String result2 =
        Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "stripFolderSuffixIfPresent", input2);
    String result3 =
        Whitebox.invokeMethod(mMockSwiftUnderFileSystem, "stripFolderSuffixIfPresent", input3);

    Assert.assertEquals(mMockContainerPrefix, result1);
    Assert.assertEquals(mMockContainerPrefix + "dir/file", result2);
    Assert.assertEquals(mMockContainerPrefix + "dir", result3);
  }

  /**
   * Tests the {@link SwiftUnderFileSystem#stripPrefixIfPresent(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    String[] inputs = new String[]{
        "swift://" + mMockContainerName,
        mMockContainerPrefix,
        mMockContainerPrefix + "file",
        mMockContainerPrefix + "dir/file",
        "swift://test-container-wrong/dir/file",
        "dir/file",
        "/dir/file",
    };
    String[] results = new String[]{
        "swift://" + mMockContainerName,
        "",
        "file",
        "dir/file",
        "swift://test-container-wrong/dir/file",
        "dir/file",
        "/dir/file",
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(results[i], Whitebox.invokeMethod(mMockSwiftUnderFileSystem,
          "stripPrefixIfPresent", inputs[i]));
    }
  }
}
