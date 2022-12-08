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
import alluxio.fuse.FuseConstants;
import alluxio.fuse.options.FuseOptions;
import alluxio.metrics.MetricsSystem;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link UpdateChecker}.
 */
public class UpdateCheckerTest {
  @Test
  public void UnderFileSystemAlluxio() {
    try (UpdateChecker checker = UpdateChecker
        .create(FuseOptions.create(Configuration.global()))) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(),
          UpdateChecker.ALLUXIO_FS));
    }
  }

  @Test
  public void UnderFileSystemLocal() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("/home/ec2-user/testFolder")) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(),
          UpdateChecker.LOCAL_FS));
    }
  }

  @Test
  public void UnderFileSystemS3() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("s3://alluxio-test/")) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(), "s3"));
    }
  }

  @Test
  public void UnderFileSystemS3A() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("s3a://alluxio-test/")) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(), "s3a"));
    }
  }

  @Test
  public void UnderFileSystemHdfs() {
    try (UpdateChecker checker = getUpdateCheckerWithUfs("hdfs://namenode:port/testFolder")) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(), "hdfs"));
    }
  }

  @Test
  public void localKernelDataCacheDisabled() {
    try (UpdateChecker checker = getUpdateCheckerWithMountOptions("direct_io")) {
      Assert.assertFalse(containsTargetInfo(checker.getUnchangeableFuseInfo(),
          UpdateChecker.LOCAL_KERNEL_DATA_CACHE));
    }
  }

  @Test
  public void localKernelDataCacheEnabled() {
    try (UpdateChecker checker = getUpdateCheckerWithMountOptions("kernel_cache")) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(),
          UpdateChecker.LOCAL_KERNEL_DATA_CACHE));
    }
    try (UpdateChecker checker = getUpdateCheckerWithMountOptions("auto_cache")) {
      Assert.assertTrue(containsTargetInfo(checker.getUnchangeableFuseInfo(),
          UpdateChecker.LOCAL_KERNEL_DATA_CACHE));
    }
  }

  @Test
  public void FuseOpsCalled() {
    try (UpdateChecker checker = UpdateChecker.create(FuseOptions.create(
        Configuration.global()))) {
      MetricsSystem.timer(FuseConstants.FUSE_READ).update(5, TimeUnit.MILLISECONDS);
      Assert.assertTrue(containsTargetInfo(checker.getFuseCheckInfo(), FuseConstants.FUSE_READ));
      MetricsSystem.timer(FuseConstants.FUSE_WRITE).update(5, TimeUnit.MILLISECONDS);
      List<String> checkInfo = checker.getFuseCheckInfo();
      Assert.assertFalse(containsTargetInfo(checkInfo, FuseConstants.FUSE_READ));
      Assert.assertTrue(containsTargetInfo(checkInfo, FuseConstants.FUSE_WRITE));
    }
  }

  private UpdateChecker getUpdateCheckerWithUfs(String ufsAddress) {
    return UpdateChecker.create(FuseOptions.create(FileSystemOptions.create(Configuration.global(),
        Optional.of(new UfsFileSystemOptions(ufsAddress))), false, Configuration.global()));
  }

  private UpdateChecker getUpdateCheckerWithMountOptions(String mountOptions) {
    InstancedConfiguration conf = Configuration.copyGlobal();
    conf.set(PropertyKey.FUSE_MOUNT_OPTIONS,
        PropertyKey.FUSE_MOUNT_OPTIONS.formatValue(mountOptions));
    return UpdateChecker.create(FuseOptions.create(conf));
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
