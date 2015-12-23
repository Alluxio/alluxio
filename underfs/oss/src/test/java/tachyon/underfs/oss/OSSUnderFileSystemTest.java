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

package tachyon.underfs.oss;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link OSSUnderFileSystem} that do not require an
 * OSS backend
 */
public class OSSUnderFileSystemTest {
  private OSSUnderFileSystem mMockOSSUnderFileSystem;

  @Before
  public  final void before() {
    mMockOSSUnderFileSystem = PowerMockito.mock(OSSUnderFileSystem.class);
    Whitebox.setInternalState(mMockOSSUnderFileSystem, "mBucketName", "test-bucket");
    Whitebox.setInternalState(mMockOSSUnderFileSystem, "mBucketPrefix", "oss://test-bucket/");
  }

  @Test
  public void convertToFolderNameTest() throws Exception {
    String input1 = "test";
    String result1 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "convertToFolderName", input1);

    Assert.assertEquals(result1, "test_$folder$");
  }

  @Test
  public void getChildNameTest() throws Exception {
    String input11 = "oss://test-bucket/child";
    String input12 = "oss://test-bucket/";
    String input21 = "oss://test-bucket/parent/child";
    String input22 = "oss://test-bucket/parent/";
    String input31 = "oss://test-bucket/child";
    String input32 = "oss://test-bucket/not-parent";
    String result1 =
        Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getChildName", input11, input12);
    String result2 =
        Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getChildName", input21, input22);
    String result3 =
        Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getChildName", input31, input32);

    Assert.assertEquals("child", result1);
    Assert.assertEquals("child", result2);
    Assert.assertNull(result3);
  }

  @Test
  public void getParentKeyTest() throws Exception {
    String input1 = "oss://test-bucket/parent-is-root";
    String input2 = "oss://test-bucket/";
    String input3 = "oss://test-bucket/parent/child";
    String input4 = "oss://test-bucket";
    String result1 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getParentKey", input1);
    String result2 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getParentKey", input2);
    String result3 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getParentKey", input3);
    String result4 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "getParentKey", input4);

    Assert.assertEquals("oss://test-bucket", result1);
    Assert.assertNull(result2);
    Assert.assertEquals("oss://test-bucket/parent", result3);
    Assert.assertNull(result4);
  }

  @Test
  public void isRootTest() throws Exception {
    String input1 = "oss://";
    String input2 = "oss://test-bucket";
    String input3 = "oss://test-bucket/";
    String input4 = "oss://test-bucket/file";
    String input5 = "oss://test-bucket/dir/file";
    String input6 = "oss://test-bucket-wrong/";
    Boolean result1 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "isRoot", input1);
    Boolean result2 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "isRoot", input2);
    Boolean result3 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "isRoot", input3);
    Boolean result4 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "isRoot", input4);
    Boolean result5 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "isRoot", input5);
    Boolean result6 = Whitebox.invokeMethod(mMockOSSUnderFileSystem, "isRoot", input6);

    Assert.assertFalse(result1);
    Assert.assertTrue(result2);
    Assert.assertTrue(result3);
    Assert.assertFalse(result4);
    Assert.assertFalse(result5);
    Assert.assertFalse(result6);
  }

  @Test
  public void stripFolderSuffixIfPresentTest() throws Exception {
    String input1 = "oss://test-bucket/";
    String input2 = "oss://test-bucket/dir/file";
    String input3 = "oss://test-bucket/dir_$folder$";
    String result1 =
        Whitebox.invokeMethod(mMockOSSUnderFileSystem, "stripFolderSuffixIfPresent", input1);
    String result2 =
        Whitebox.invokeMethod(mMockOSSUnderFileSystem, "stripFolderSuffixIfPresent", input2);
    String result3 =
        Whitebox.invokeMethod(mMockOSSUnderFileSystem, "stripFolderSuffixIfPresent", input3);

    Assert.assertEquals("oss://test-bucket/", result1);
    Assert.assertEquals("oss://test-bucket/dir/file", result2);
    Assert.assertEquals("oss://test-bucket/dir", result3);
  }

  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    String[] inputs = new String[]{
        "oss://test-bucket",
        "oss://test-bucket/",
        "oss://test-bucket/file",
        "oss://test-bucket/dir/file",
        "oss://test-bucket-wrong/dir/file",
        "dir/file",
        "/dir/file",
    };
    String[] results = new String[]{
        "oss://test-bucket",
        "",
        "file",
        "dir/file",
        "oss://test-bucket-wrong/dir/file",
        "dir/file",
        "dir/file",
    };
    for (int i = 0; i < inputs.length; i ++) {
      Assert.assertEquals(results[i], Whitebox.invokeMethod(mMockOSSUnderFileSystem,
          "stripPrefixIfPresent", inputs[i]));
    }
  }
}
