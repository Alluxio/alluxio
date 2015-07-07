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

package tachyon.underfs.s3;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link tachyon.underfs.s3.S3UnderFileSystem} that do
 * not require an S3 backend
 */
public class S3UnderFileSystemTest {
  private static S3UnderFileSystem sMockS3UnderFileSystem;

  @BeforeClass
  public static final void beforeClass() {
    sMockS3UnderFileSystem = PowerMockito.mock(S3UnderFileSystem.class);
    Whitebox.setInternalState(sMockS3UnderFileSystem, "mBucketName", "test-bucket");
    Whitebox.setInternalState(sMockS3UnderFileSystem, "mBucketPrefix", "s3n://test-bucket/");
  }

  @Test
  public void convertToFolderNameTest() throws Exception {
    String input1 = "test";
    String result1 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "convertToFolderName", input1);

    Assert.assertEquals(result1, "test_$folder$");
  }

  @Test
  public void getChildNameTest() throws Exception {
    String input11 = "s3n://test-bucket/child";
    String input12 = "s3n://test-bucket/";
    String input21 = "s3n://test-bucket/parent/child";
    String input22 = "s3n://test-bucket/parent/";
    String input31 = "s3n://test-bucket/child";
    String input32 = "s3n://test-bucket/not-parent";
    String result1 =
        Whitebox.invokeMethod(sMockS3UnderFileSystem, "getChildName", input11, input12);
    String result2 =
        Whitebox.invokeMethod(sMockS3UnderFileSystem, "getChildName", input21, input22);
    String result3 =
        Whitebox.invokeMethod(sMockS3UnderFileSystem, "getChildName", input31, input32);

    Assert.assertEquals("child", result1);
    Assert.assertEquals("child", result2);
    Assert.assertNull(result3);
  }

  @Test
  public void getParentKeyTest() throws Exception {
    String input1 = "s3n://test-bucket/parent-is-root";
    String input2 = "s3n://test-bucket/";
    String input3 = "s3n://test-bucket/parent/child";
    String input4 = "s3n://test-bucket";
    String result1 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "getParentKey", input1);
    String result2 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "getParentKey", input2);
    String result3 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "getParentKey", input3);
    String result4 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "getParentKey", input4);

    Assert.assertEquals("s3n://test-bucket", result1);
    Assert.assertNull(result2);
    Assert.assertEquals("s3n://test-bucket/parent", result3);
    Assert.assertNull(result4);
  }

  @Test
  public void isRootTest() throws Exception {
    String input1 = "s3n://";
    String input2 = "s3n://test-bucket";
    String input3 = "s3n://test-bucket/";
    String input4 = "s3n://test-bucket/file";
    String input5 = "s3n://test-bucket/dir/file";
    String input6 = "s3n://test-bucket-wrong/";
    Boolean result1 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "isRoot", input1);
    Boolean result2 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "isRoot", input2);
    Boolean result3 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "isRoot", input3);
    Boolean result4 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "isRoot", input4);
    Boolean result5 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "isRoot", input5);
    Boolean result6 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "isRoot", input6);

    Assert.assertFalse(result1);
    Assert.assertTrue(result2);
    Assert.assertTrue(result3);
    Assert.assertFalse(result4);
    Assert.assertFalse(result5);
    Assert.assertFalse(result6);
  }

  @Test
  public void stripFolderSuffixIfPresentTest() throws Exception {
    String input1 = "s3n://test-bucket/";
    String input2 = "s3n://test-bucket/dir/file";
    String input3 = "s3n://test-bucket/dir_$folder$";
    String result1 =
        Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripFolderSuffixIfPresent", input1);
    String result2 =
        Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripFolderSuffixIfPresent", input2);
    String result3 =
        Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripFolderSuffixIfPresent", input3);

    Assert.assertEquals("s3n://test-bucket/", result1);
    Assert.assertEquals("s3n://test-bucket/dir/file", result2);
    Assert.assertEquals("s3n://test-bucket/dir", result3);
  }

  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    String input1 = "s3n://test-bucket";
    String input2 = "s3n://test-bucket/";
    String input3 = "s3n://test-bucket/file";
    String input4 = "s3n://test-bucket/dir/file";
    String input5 = "s3n://test-bucket-wrong/dir/file";
    String input6 = "dir/file";
    String result1 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripPrefixIfPresent", input1);
    String result2 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripPrefixIfPresent", input2);
    String result3 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripPrefixIfPresent", input3);
    String result4 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripPrefixIfPresent", input4);
    String result5 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripPrefixIfPresent", input5);
    String result6 = Whitebox.invokeMethod(sMockS3UnderFileSystem, "stripPrefixIfPresent", input6);

    Assert.assertEquals("s3n://test-bucket", result1);
    Assert.assertEquals("", result2);
    Assert.assertEquals("file", result3);
    Assert.assertEquals("dir/file", result4);
    Assert.assertEquals("s3n://test-bucket-wrong/dir/file", result5);
    Assert.assertEquals("dir/file", result6);
  }

}
