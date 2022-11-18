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
import alluxio.fuse.FuseConstants;
import alluxio.fuse.options.FuseOptions;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.metrics.MetricsSystem;
import alluxio.util.URIUtils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically Alluxio version update check.
 */
@NotThreadSafe
public final class UpdateChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateChecker.class);
  static final String UNDERLYING_FS_FORMAT = "UnderlyingFileSystem:%s";
  static final String LOCAL_ALLUXIO_DATA_CACHE = "localAlluxioDataCache";
  static final String LOCAL_ALLUXIO_METADATA_CACHE = "localAlluxioMetadataCache";
  static final String LOCAL_KERNEL_DATA_CACHE = "localKernelDataCache";

  static final String ALLUXIO_FS = "alluxio";
  static final String LOCAL_FS = "local";

  private final String mInstanceId = UUID.randomUUID().toString();

  private final Map<String, Long> mFuseOpsCounter;
  private final List<String> mUnchangeableFuseInfo;

  /**
   * Creates a {@link UpdateChecker}.
   *
   * @param fuseOptions the fuse options
   * @return the update checker
   */
  public static UpdateChecker create(FuseOptions fuseOptions) {
    List<String> fuseInfo = new ArrayList<>();
    UpdateCheck.addIfTrue(isLocalAlluxioDataCacheEnabled(fuseOptions),
        fuseInfo, LOCAL_ALLUXIO_DATA_CACHE);
    UpdateCheck.addIfTrue(isLocalAlluxioMetadataCacheEnabled(fuseOptions), fuseInfo,
        LOCAL_ALLUXIO_METADATA_CACHE);
    UpdateCheck.addIfTrue(isLocalKernelDataCacheEnabled(fuseOptions),
        fuseInfo, LOCAL_KERNEL_DATA_CACHE);
    fuseInfo.add(String.format("UnderlyingFileSystem:%s", getUnderlyingFileSystem(fuseOptions)));
    return new UpdateChecker(Collections.unmodifiableList(fuseInfo),
        FuseConstants.getFuseMethodNames().stream()
            .collect(Collectors.toMap(methodName -> methodName, methodName -> 0L)));
  }

  private UpdateChecker(List<String> unchangeableFuseInfo, Map<String, Long> fuseOpsCounter) {
    mUnchangeableFuseInfo = unchangeableFuseInfo;
    mFuseOpsCounter = fuseOpsCounter;
  }

  /**
   * Heartbeat for the periodic update check.
   */
  @Override
  public void heartbeat() {
    try {
      String latestVersion =
          UpdateCheck.getLatestVersion(mInstanceId, getFuseCheckInfo(),
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
  List<String> getFuseCheckInfo() {
    List<String> info = new ArrayList<>(mUnchangeableFuseInfo);
    for (String fuseOps : mFuseOpsCounter.keySet()) {
      mFuseOpsCounter.computeIfPresent(fuseOps, (key, value) -> {
        long newCount = MetricsSystem.timer(key).getCount();
        if (newCount > value) {
          info.add(fuseOps);
        }
        return newCount;
      });
    }
    return info;
  }

  /**
   * @return
   */
  @VisibleForTesting
  List<String> getUnchangeableFuseInfo() {
    return mUnchangeableFuseInfo;
  }

  /**
   * @return true, if local Alluxio data cache is enabled
   */
  private static boolean isLocalAlluxioDataCacheEnabled(FuseOptions fuseOptions) {
    return fuseOptions.getFileSystemOptions().isDataCacheEnabled();
  }

  /**
   * @return true, if local Alluxio metadata cache is enabled
   */
  private static boolean isLocalAlluxioMetadataCacheEnabled(FuseOptions fuseOptions) {
    return fuseOptions.getFileSystemOptions().isMetadataCacheEnabled();
  }

  /**
   * @return true, if local kernel data cache is enabled
   */
  private static  boolean isLocalKernelDataCacheEnabled(FuseOptions fuseOptions) {
    return !fuseOptions.getFuseMountOptions().contains("direct_io");
  }

  /**
   * @return the underlying file system type
   */
  private static String getUnderlyingFileSystem(FuseOptions fuseOptions) {
    if (!fuseOptions.getFileSystemOptions().getUfsFileSystemOptions().isPresent()) {
      return ALLUXIO_FS;
    }
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
}
