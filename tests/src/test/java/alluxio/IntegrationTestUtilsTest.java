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

package alluxio;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.gcs.GCSUnderFileSystem;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.oss.OSSUnderFileSystem;
import alluxio.underfs.s3.S3UnderFileSystem;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.underfs.swift.SwiftUnderFileSystem;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link IntegrationTestUtils}.
 */
public class IntegrationTestUtilsTest {
  @Test
  public void gcs() {
    UnderFileSystem underFileSystem = Mockito.mock(GCSUnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertTrue(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }

  @Test
  public void hdfs() {
    UnderFileSystem underFileSystem = Mockito.mock(HdfsUnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertFalse(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }

  @Test
  public void local() {
    UnderFileSystem underFileSystem = Mockito.mock(LocalUnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertFalse(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }

  @Test
  public void oss() {
    UnderFileSystem underFileSystem = Mockito.mock(OSSUnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertFalse(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }

  @Test
  public void s3() {
    UnderFileSystem underFileSystem = Mockito.mock(S3UnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertFalse(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }

  @Test
  public void s3a() {
    UnderFileSystem underFileSystem = Mockito.mock(S3AUnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertFalse(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }

  @Test
  public void swift() {
    UnderFileSystem underFileSystem = Mockito.mock(SwiftUnderFileSystem.class);
    Mockito.when(underFileSystem.getUnderFSType()).thenCallRealMethod();

    Assert.assertFalse(IntegrationTestUtils.isGcs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isHdfs(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isLocal(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isOss(underFileSystem));
    Assert.assertFalse(IntegrationTestUtils.isS3(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isSwift(underFileSystem));
    Assert.assertTrue(IntegrationTestUtils.isObjectStorage(underFileSystem));
  }
}
