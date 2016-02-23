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

package alluxio.underfs.s3;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link S3UnderFileSystem} that do not require an S3
 * backend.
 */
public class S3UnderFileSystemTest {
  private S3UnderFileSystem mMockS3UnderFileSystem;

  /**
   * Sets up the mock before a test runs.
   */
  @Before
  public  final void before() {
    mMockS3UnderFileSystem = PowerMockito.mock(S3UnderFileSystem.class);
    Whitebox.setInternalState(mMockS3UnderFileSystem, "mBucketName", "test-bucket");
    Whitebox.setInternalState(mMockS3UnderFileSystem, "mBucketPrefix", "s3n://test-bucket/");
  }

  /**
   * Tests the {@link S3UnderFileSystem#convertToFolderName(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void convertToFolderNameTest() throws Exception {
    String input1 = "test";
    String result1 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "convertToFolderName", input1);

    Assert.assertEquals(result1, "test_$folder$");
  }

  /**
   * Tests the {@link S3UnderFileSystem#getChildName(String, String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void getChildNameTest() throws Exception {
    String input11 = "s3n://test-bucket/child";
    String input12 = "s3n://test-bucket/";
    String input21 = "s3n://test-bucket/parent/child";
    String input22 = "s3n://test-bucket/parent/";
    String input31 = "s3n://test-bucket/child";
    String input32 = "s3n://test-bucket/not-parent";
    String result1 =
        Whitebox.invokeMethod(mMockS3UnderFileSystem, "getChildName", input11, input12);
    String result2 =
        Whitebox.invokeMethod(mMockS3UnderFileSystem, "getChildName", input21, input22);
    String result3 =
        Whitebox.invokeMethod(mMockS3UnderFileSystem, "getChildName", input31, input32);

    Assert.assertEquals("child", result1);
    Assert.assertEquals("child", result2);
    Assert.assertNull(result3);
  }

  /**
   * Tests the {@link S3UnderFileSystem#getParentKey(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void getParentKeyTest() throws Exception {
    String input1 = "s3n://test-bucket/parent-is-root";
    String input2 = "s3n://test-bucket/";
    String input3 = "s3n://test-bucket/parent/child";
    String input4 = "s3n://test-bucket";
    String result1 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input1);
    String result2 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input2);
    String result3 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input3);
    String result4 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input4);

    Assert.assertEquals("s3n://test-bucket", result1);
    Assert.assertNull(result2);
    Assert.assertEquals("s3n://test-bucket/parent", result3);
    Assert.assertNull(result4);
  }

  /**
   * Tests the {@link S3UnderFileSystem#isRoot(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void isRootTest() throws Exception {
    String input1 = "s3n://";
    String input2 = "s3n://test-bucket";
    String input3 = "s3n://test-bucket/";
    String input4 = "s3n://test-bucket/file";
    String input5 = "s3n://test-bucket/dir/file";
    String input6 = "s3n://test-bucket-wrong/";
    Boolean result1 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "isRoot", input1);
    Boolean result2 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "isRoot", input2);
    Boolean result3 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "isRoot", input3);
    Boolean result4 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "isRoot", input4);
    Boolean result5 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "isRoot", input5);
    Boolean result6 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "isRoot", input6);

    Assert.assertFalse(result1);
    Assert.assertTrue(result2);
    Assert.assertTrue(result3);
    Assert.assertFalse(result4);
    Assert.assertFalse(result5);
    Assert.assertFalse(result6);
  }

  /**
   * Tests the {@link S3UnderFileSystem#stripFolderSuffixIfPresent(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void stripFolderSuffixIfPresentTest() throws Exception {
    String input1 = "s3n://test-bucket/";
    String input2 = "s3n://test-bucket/dir/file";
    String input3 = "s3n://test-bucket/dir_$folder$";
    String result1 =
        Whitebox.invokeMethod(mMockS3UnderFileSystem, "stripFolderSuffixIfPresent", input1);
    String result2 =
        Whitebox.invokeMethod(mMockS3UnderFileSystem, "stripFolderSuffixIfPresent", input2);
    String result3 =
        Whitebox.invokeMethod(mMockS3UnderFileSystem, "stripFolderSuffixIfPresent", input3);

    Assert.assertEquals("s3n://test-bucket/", result1);
    Assert.assertEquals("s3n://test-bucket/dir/file", result2);
    Assert.assertEquals("s3n://test-bucket/dir", result3);
  }

  /**
   * Tests the {@link S3UnderFileSystem#stripPrefixIfPresent(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    String[] inputs = new String[]{
        "s3n://test-bucket",
        "s3n://test-bucket/",
        "s3n://test-bucket/file",
        "s3n://test-bucket/dir/file",
        "s3n://test-bucket-wrong/dir/file",
        "dir/file",
        "/dir/file",
    };
    String[] results = new String[]{
        "s3n://test-bucket",
        "",
        "file",
        "dir/file",
        "s3n://test-bucket-wrong/dir/file",
        "dir/file",
        "dir/file",
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(results[i], Whitebox.invokeMethod(mMockS3UnderFileSystem,
          "stripPrefixIfPresent", inputs[i]));
    }
  }
}
