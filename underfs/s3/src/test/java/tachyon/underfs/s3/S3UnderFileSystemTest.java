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
  }

  @Test
  public void convertToFolderNameTest() throws Exception {
    String result = Whitebox.invokeMethod(sMockS3UnderFileSystem, "convertToFolderName", "test");
    Assert.assertEquals(result, "test_$folder$");
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

    Assert.assertEquals(result1, "s3n://test-bucket");
    Assert.assertEquals(result2, null);
    Assert.assertEquals(result3, "s3n://test-bucket/parent");
    Assert.assertEquals(result4, null);
  }

}
