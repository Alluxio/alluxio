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

package alluxio.fuse.meta;

import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.fuse.options.FuseOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Tests for {@link UpdateChecker}.
 */
public class UpdateCheckerTest {
  @Test
  public void UnderFileSystemAlluxio() {
    try (UpdateChecker checker = new UpdateChecker(FuseOptions.create(Configuration.global()))) {
      Assert.assertEquals(UpdateChecker.ALLUXIO_FS, checker.getUnderlyingFileSystem());
    }
  }

  @Test
  public void UnderFileSystemLocal() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("/home/ec2-user/testFolder")) {
      Assert.assertEquals(UpdateChecker.LOCAL_FS, checker.getUnderlyingFileSystem());
    }
  }

  @Test
  public void UnderFileSystemS3() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("s3://alluxio-test/")) {
      Assert.assertEquals("s3", checker.getUnderlyingFileSystem());
    }
  }

  @Test
  public void UnderFileSystemS3A() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("s3a://alluxio-test/")) {
      Assert.assertEquals("s3a", checker.getUnderlyingFileSystem());
    }
  }

  @Test
  public void UnderFileSystemHDFS() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("hdfs://namenode:port/testFolder")) {
      Assert.assertEquals("hdfs", checker.getUnderlyingFileSystem());
    }
  }

  @Test
  public void localKernelDataCacheDisabled() {
    try (UpdateChecker checker = getUpdateCheckerWithMountOptions("direct_io")) {
      Assert.assertFalse(checker.isLocalKernelDataCacheEnabled());
    }
  }

  @Test
  public void localKernelDataCacheEnabled() {
    try (UpdateChecker checker = getUpdateCheckerWithMountOptions("kernel_cache")) {
      Assert.assertTrue(checker.isLocalKernelDataCacheEnabled());
    }
    try (UpdateChecker checker = getUpdateCheckerWithMountOptions("auto_cache")) {
      Assert.assertTrue(checker.isLocalKernelDataCacheEnabled());
    }
  }

  private UpdateChecker getUpdateCheckerWithUfs(String ufsAddress) {
    return new UpdateChecker(FuseOptions.create(Configuration.global(),
        FileSystemOptions.create(Configuration.global(),
            Optional.of(new UfsFileSystemOptions(ufsAddress))), false));
  }

  private UpdateChecker getUpdateCheckerWithMountOptions(String mountOptions) {
    InstancedConfiguration conf = Configuration.copyGlobal();
    conf.set(PropertyKey.FUSE_MOUNT_OPTIONS,
        PropertyKey.FUSE_MOUNT_OPTIONS.formatValue(mountOptions));
    return new UpdateChecker(FuseOptions.create(conf));
  }
}
