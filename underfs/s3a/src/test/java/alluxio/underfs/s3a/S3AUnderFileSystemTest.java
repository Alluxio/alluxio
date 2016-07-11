/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.s3a;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link S3AUnderFileSystem} that do not require an S3
 * backend.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(S3AUnderFileSystem.class)
public final class S3AUnderFileSystemTest {
  private S3AUnderFileSystem mMockS3UnderFileSystem;

  /**
   * Sets up the mock before a test runs.
   */
  @Before
  public  final void before() {
    mMockS3UnderFileSystem = Mockito.mock(S3AUnderFileSystem.class);
    Whitebox.setInternalState(mMockS3UnderFileSystem, "mBucketName", "test-bucket");
    Whitebox.setInternalState(mMockS3UnderFileSystem, "mBucketPrefix", "s3a://test-bucket/");
  }

  /**
   * Tests the {@link S3AUnderFileSystem#convertToFolderName(String)} method.
   */
  @Test
  public void convertToFolderNameTest() throws Exception {
    String input1 = "test";
    String result1 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "convertToFolderName", input1);

    Assert.assertEquals(result1, "test_$folder$");
  }

  /**
   * Tests the {@link S3AUnderFileSystem#getChildName(String, String)} method.
   */
  @Test
  public void getChildNameTest() throws Exception {
    String input11 = "s3a://test-bucket/child";
    String input12 = "s3a://test-bucket/";
    String input21 = "s3a://test-bucket/parent/child";
    String input22 = "s3a://test-bucket/parent/";
    String input31 = "s3a://test-bucket/child";
    String input32 = "s3a://test-bucket/not-parent";
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
   * Tests the {@link S3AUnderFileSystem#getParentKey(String)} method.
   */
  @Test
  public void getParentKeyTest() throws Exception {
    String input1 = "s3a://test-bucket/parent-is-root";
    String input2 = "s3a://test-bucket/";
    String input3 = "s3a://test-bucket/parent/child";
    String input4 = "s3a://test-bucket";
    String result1 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input1);
    String result2 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input2);
    String result3 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input3);
    String result4 = Whitebox.invokeMethod(mMockS3UnderFileSystem, "getParentKey", input4);

    Assert.assertEquals("s3a://test-bucket", result1);
    Assert.assertNull(result2);
    Assert.assertEquals("s3a://test-bucket/parent", result3);
    Assert.assertNull(result4);
  }

  /**
   * Tests the {@link S3AUnderFileSystem#isRoot(String)} method.
   */
  @Test
  public void isRootTest() throws Exception {
    String input1 = "s3a://";
    String input2 = "s3a://test-bucket";
    String input3 = "s3a://test-bucket/";
    String input4 = "s3a://test-bucket/file";
    String input5 = "s3a://test-bucket/dir/file";
    String input6 = "s3a://test-bucket-wrong/";
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
   * Tests the {@link S3AUnderFileSystem#stripPrefixIfPresent(String)} method.
   */
  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    String[] inputs = new String[]{
        "s3a://test-bucket",
        "s3a://test-bucket/",
        "s3a://test-bucket/file",
        "s3a://test-bucket/dir/file",
        "s3a://test-bucket-wrong/dir/file",
        "dir/file",
        "/dir/file",
    };
    String[] results = new String[]{
        "s3a://test-bucket",
        "",
        "file",
        "dir/file",
        "s3a://test-bucket-wrong/dir/file",
        "dir/file",
        "dir/file",
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(results[i], Whitebox.invokeMethod(mMockS3UnderFileSystem,
          "stripPrefixIfPresent", inputs[i]));
    }
  }
}
