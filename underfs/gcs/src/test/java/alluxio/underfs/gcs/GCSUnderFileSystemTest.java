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

package alluxio.underfs.gcs;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for the private helper methods in {@link GCSUnderFileSystem} that do not require an GCS
 * backend.
 */
public final class GCSUnderFileSystemTest {
  private GCSUnderFileSystem mMockGCSUnderFileSystem;

  /**
   * Sets up the mock before a test runs.
   */
  @Before
  public  final void before() {
    mMockGCSUnderFileSystem = PowerMockito.mock(GCSUnderFileSystem.class);
    Whitebox.setInternalState(mMockGCSUnderFileSystem, "mBucketName", "test-bucket");
    Whitebox.setInternalState(mMockGCSUnderFileSystem, "mBucketPrefix", "gs://test-bucket/");
  }

  /**
   * Tests the {@link GCSUnderFileSystem#convertToFolderName(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void convertToFolderNameTest() throws Exception {
    String input1 = "test";
    String result1 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "convertToFolderName", input1);

    Assert.assertEquals(result1, "test_$folder$");
  }

  /**
   * Tests the {@link GCSUnderFileSystem#getChildName(String, String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void getChildNameTest() throws Exception {
    String input11 = "gs://test-bucket/child";
    String input12 = "gs://test-bucket/";
    String input21 = "gs://test-bucket/parent/child";
    String input22 = "gs://test-bucket/parent/";
    String input31 = "gs://test-bucket/child";
    String input32 = "gs://test-bucket/not-parent";
    String result1 =
        Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getChildName", input11, input12);
    String result2 =
        Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getChildName", input21, input22);
    String result3 =
        Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getChildName", input31, input32);

    Assert.assertEquals("child", result1);
    Assert.assertEquals("child", result2);
    Assert.assertNull(result3);
  }

  /**
   * Tests the {@link GCSUnderFileSystem#getParentKey(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void getParentKeyTest() throws Exception {
    String input1 = "gs://test-bucket/parent-is-root";
    String input2 = "gs://test-bucket/";
    String input3 = "gs://test-bucket/parent/child";
    String input4 = "gs://test-bucket";
    String result1 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getParentKey", input1);
    String result2 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getParentKey", input2);
    String result3 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getParentKey", input3);
    String result4 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "getParentKey", input4);

    Assert.assertEquals("gs://test-bucket", result1);
    Assert.assertNull(result2);
    Assert.assertEquals("gs://test-bucket/parent", result3);
    Assert.assertNull(result4);
  }

  /**
   * Tests the {@link GCSUnderFileSystem#isRoot(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void isRootTest() throws Exception {
    String input1 = "gs://";
    String input2 = "gs://test-bucket";
    String input3 = "gs://test-bucket/";
    String input4 = "gs://test-bucket/file";
    String input5 = "gs://test-bucket/dir/file";
    String input6 = "gs://test-bucket-wrong/";
    Boolean result1 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "isRoot", input1);
    Boolean result2 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "isRoot", input2);
    Boolean result3 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "isRoot", input3);
    Boolean result4 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "isRoot", input4);
    Boolean result5 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "isRoot", input5);
    Boolean result6 = Whitebox.invokeMethod(mMockGCSUnderFileSystem, "isRoot", input6);

    Assert.assertFalse(result1);
    Assert.assertTrue(result2);
    Assert.assertTrue(result3);
    Assert.assertFalse(result4);
    Assert.assertFalse(result5);
    Assert.assertFalse(result6);
  }

  /**
   * Tests the {@link GCSUnderFileSystem#stripFolderSuffixIfPresent(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void stripFolderSuffixIfPresentTest() throws Exception {
    String input1 = "gs://test-bucket/";
    String input2 = "gs://test-bucket/dir/file";
    String input3 = "gs://test-bucket/dir_$folder$";
    String result1 =
        Whitebox.invokeMethod(mMockGCSUnderFileSystem, "stripFolderSuffixIfPresent", input1);
    String result2 =
        Whitebox.invokeMethod(mMockGCSUnderFileSystem, "stripFolderSuffixIfPresent", input2);
    String result3 =
        Whitebox.invokeMethod(mMockGCSUnderFileSystem, "stripFolderSuffixIfPresent", input3);

    Assert.assertEquals("gs://test-bucket/", result1);
    Assert.assertEquals("gs://test-bucket/dir/file", result2);
    Assert.assertEquals("gs://test-bucket/dir", result3);
  }

  /**
   * Tests the {@link GCSUnderFileSystem#stripPrefixIfPresent(String)} method.
   *
   * @throws Exception when the Whitebox fails
   */
  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    String[] inputs = new String[]{
        "gs://test-bucket",
        "gs://test-bucket/",
        "gs://test-bucket/file",
        "gs://test-bucket/dir/file",
        "gs://test-bucket-wrong/dir/file",
        "dir/file",
        "/dir/file",
    };
    String[] results = new String[]{
        "gs://test-bucket",
        "",
        "file",
        "dir/file",
        "gs://test-bucket-wrong/dir/file",
        "dir/file",
        "dir/file",
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(results[i], Whitebox.invokeMethod(mMockGCSUnderFileSystem,
          "stripPrefixIfPresent", inputs[i]));
    }
  }
}
