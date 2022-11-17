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
import alluxio.check.UpdateCheck;
import alluxio.fuse.options.FuseOptions;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.util.URIUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically Alluxio version update check.
 */
@NotThreadSafe
public final class UpdateChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateChecker.class);

  static final String ALLUXIO_FS = "alluxio";
  static final String LOCAL_FS = "local";

  private final FuseOptions mFuseOptions;

  private String mInstanceId;
  private List<String> mUnchangeableFuseInfo;

  /**
   * Creates a new instance of {@link UpdateChecker}.
   *
   * @param fuseOptions the fuse options
   */
  public UpdateChecker(FuseOptions fuseOptions) {
    mFuseOptions = fuseOptions;
  }

  /**
   * Heartbeat for the periodic update check.
   */
  @Override
  public void heartbeat() {
    if (mInstanceId == null) {
      mInstanceId = getNewInstanceId();
      mUnchangeableFuseInfo = getUnchangeableFuseInfo();
    }
    try {
      String latestVersion =
          UpdateCheck.getLatestVersion(mInstanceId, mUnchangeableFuseInfo,
              3000, 3000, 3000);
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

  @VisibleForTesting
  List<String> getUnchangeableFuseInfo() {
    List<String> fuseInfo = new ArrayList<>();
    UpdateCheck.addIfTrue(isLocalAlluxioDataCacheEnabled(), fuseInfo, "localAlluxioDataCache");
    UpdateCheck.addIfTrue(isLocalAlluxioMetadataCacheEnabled(), fuseInfo,
        "localAlluxioMetadataCache");
    UpdateCheck.addIfTrue(isLocalKernelDataCacheEnabled(), fuseInfo, "localKernelDataCache");
    fuseInfo.add(String.format("UnderlyingFileSystem:%s", getUnderlyingFileSystem()));
    return Collections.unmodifiableList(fuseInfo);
  }

  private String getNewInstanceId() {
    return UUID.randomUUID().toString();
  }

  /**
   * @return true, if local Alluxio data cache is enabled
   */
  private boolean isLocalAlluxioDataCacheEnabled() {
    return mFuseOptions.getFileSystemOptions().isDataCacheEnabled();
  }

  /**
   * @return true, if local Alluxio metadata cache is enabled
   */
  private boolean isLocalAlluxioMetadataCacheEnabled() {
    return mFuseOptions.getFileSystemOptions().isMetadataCacheEnabled();
  }

  /**
   * @return true, if local kernel data cache is enabled
   */
  @VisibleForTesting
  boolean isLocalKernelDataCacheEnabled() {
    return !mFuseOptions.getFuseMountOptions().contains("direct_io");
  }

  /**
   * @return true, if local kernel data cache is enabled
   */
  @VisibleForTesting
  String getUnderlyingFileSystem() {
    if (!mFuseOptions.getFileSystemOptions().getUfsFileSystemOptions().isPresent()) {
      return ALLUXIO_FS;
    }
    String ufsAddress = mFuseOptions.getFileSystemOptions()
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
}
