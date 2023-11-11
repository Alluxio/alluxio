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

import alluxio.ProjectConstants;
import alluxio.fuse.options.FuseOptions;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.util.CommonUtils;
import alluxio.util.URIUtils;
import alluxio.util.UpdateCheckUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically Alluxio version update check.
 */
@NotThreadSafe
public final class FuseUpdateChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FuseUpdateChecker.class);
  static final String LOCAL_ALLUXIO_DATA_CACHE = "localAlluxioDataCache";
  static final String LOCAL_ALLUXIO_METADATA_CACHE = "localAlluxioMetadataCache";
  static final String LOCAL_KERNEL_DATA_CACHE = "localKernelDataCache";
  static final String LOCAL_FS = "local";
  private final String mInstanceId = UUID.randomUUID().toString();
  private final List<String> mFuseInfo = new ArrayList<>();

  /**
   * Creates a {@link FuseUpdateChecker}.
   *
   * @param fuseOptions the fuse options
   */
  public FuseUpdateChecker(FuseOptions fuseOptions) {
    if (fuseOptions.getFileSystemOptions().isDataCacheEnabled()) {
      mFuseInfo.add(LOCAL_ALLUXIO_DATA_CACHE);
    }
    if (fuseOptions.getFileSystemOptions().isMetadataCacheEnabled()) {
      mFuseInfo.add(LOCAL_ALLUXIO_METADATA_CACHE);
    }
    if (!fuseOptions.getFuseMountOptions().contains("direct_io")) {
      mFuseInfo.add(LOCAL_KERNEL_DATA_CACHE);
    }
    mFuseInfo.add(String.format("UnderlyingFileSystem:%s", getUnderlyingFileSystem(fuseOptions)));
  }

  /**
   * Heartbeat for the periodic update check.
   */
  @Override
  public void heartbeat(long timeLimitMs) {
    try {
      String latestVersion = UpdateCheckUtils.getLatestVersion(mInstanceId,
          CommonUtils.ProcessType.FUSE, mFuseInfo);
      if (!ProjectConstants.VERSION.equals(latestVersion)) {
        LOG.info("The latest version (" + latestVersion + ") is not the same "
            + "as the current version (" + ProjectConstants.VERSION + "). To upgrade "
            + "visit https://www.alluxio.io/download/.");
      }
    } catch (Throwable t) {
      LOG.debug("Unable to check for updates:", t);
    }
  }

  @Override
  public void close() {}

  /**
   * @return the underlying file system type
   */
  private static String getUnderlyingFileSystem(FuseOptions fuseOptions) {
    String ufsAddress = fuseOptions.getFileSystemOptions()
        .getUfsFileSystemOptions().get().getUfsAddress();
    if (URIUtils.isLocalFilesystem(ufsAddress)) {
      return LOCAL_FS;
    }
    String[] components = ufsAddress.split(":");
    if (components.length < 2) {
      return "unknown";
    }
    return components[0];
  }

  /**
   * @return
   */
  @VisibleForTesting
  List<String> getFuseInfo() {
    return mFuseInfo;
  }
}
