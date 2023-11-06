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
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link FuseUpdateChecker}.
 */
public class FuseUpdateCheckerTest {
  private final InstancedConfiguration mConf = Configuration.copyGlobal();

  @Test
  public void UnderFileSystemLocal() {
    try (FuseUpdateChecker checker = getUpdateCheckerWithUfs("/home/ec2-user/testFolder")) {
      Assert.assertTrue(containsTargetInfo(checker.getFuseInfo(),
          FuseUpdateChecker.LOCAL_FS));
    }
  }

  @Test
  public void UnderFileSystemS3() {
    try (FuseUpdateChecker checker = getUpdateCheckerWithUfs("s3://alluxio-test/")) {
      Assert.assertTrue(containsTargetInfo(checker.getFuseInfo(), "s3"));
    }
  }

  @Test
  public void UnderFileSystemS3A() {
    try (FuseUpdateChecker checker = getUpdateCheckerWithUfs("s3a://alluxio-test/")) {
      Assert.assertTrue(containsTargetInfo(checker.getFuseInfo(), "s3a"));
    }
  }

  @Test
  public void UnderFileSystemHdfs() {
    try (FuseUpdateChecker checker = getUpdateCheckerWithUfs("hdfs://namenode:port/testFolder")) {
      Assert.assertTrue(containsTargetInfo(checker.getFuseInfo(), "hdfs"));
    }
  }

  @Test
  public void localKernelDataCacheDisabled() {
    Assume.assumeTrue(Configuration.getInt(PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION) == 2);
    try (FuseUpdateChecker checker = getUpdateCheckerWithMountOptions("direct_io")) {
      Assert.assertFalse(containsTargetInfo(checker.getFuseInfo(),
          FuseUpdateChecker.LOCAL_KERNEL_DATA_CACHE));
    }
  }

  @Test
  public void localKernelDataCacheEnabled() {
    try (FuseUpdateChecker checker = getUpdateCheckerWithMountOptions("kernel_cache")) {
      Assert.assertTrue(containsTargetInfo(checker.getFuseInfo(),
          FuseUpdateChecker.LOCAL_KERNEL_DATA_CACHE));
    }
    try (FuseUpdateChecker checker = getUpdateCheckerWithMountOptions("auto_cache")) {
      Assert.assertTrue(containsTargetInfo(checker.getFuseInfo(),
          FuseUpdateChecker.LOCAL_KERNEL_DATA_CACHE));
    }
  }

  private FuseUpdateChecker getUpdateCheckerWithUfs(String ufsAddress) {
    final FileSystemOptions fileSystemOptions =
        FileSystemOptions.Builder
            .fromConf(mConf)
            .setUfsFileSystemOptions(new UfsFileSystemOptions(ufsAddress))
            .build();
    return new FuseUpdateChecker(
        FuseOptions.Builder.fromConfig(Configuration.global())
            .setFileSystemOptions(fileSystemOptions)
            .build());
  }

  private FuseUpdateChecker getUpdateCheckerWithMountOptions(String mountOptions) {
    mConf.set(PropertyKey.FUSE_MOUNT_OPTIONS,
        PropertyKey.FUSE_MOUNT_OPTIONS.formatValue(mountOptions));
    return new FuseUpdateChecker(FuseOptions.Builder.fromConfig(mConf).build());
  }

  private boolean containsTargetInfo(List<String> list, String targetInfo) {
    for (String info : list) {
      if (info.contains(targetInfo)) {
        return true;
      }
    }
    return false;
  }
}
