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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.SyncInfo;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnimplementedException;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.SecurityUtils;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * This class forwards all calls to the {@link UnderFileSystem} interface to an internal
 * implementation. For methods which throw an {@link IOException}, it is implied that an
 * interaction with the underlying storage is possible. This class logs the enter/exit of all
 * such methods. Methods which do not throw exceptions will not be logged.
 */
public class UnderFileSystemWithLogging implements UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemWithLogging.class);
  private static final String NAME_SEPARATOR = ":";

  private final UnderFileSystem mUnderFileSystem;
  private final UnderFileSystemConfiguration mConf;
  private final String mPath;
  private final String mEscapedPath;
  private final long mLoggingThreshold;

  /**
   * Creates a new {@link UnderFileSystemWithLogging} which forwards all calls to the provided
   * {@link UnderFileSystem} implementation.
   *
   * @param path the UFS path
   * @param ufs the implementation which will handle all the calls
   * @param conf Alluxio configuration
   *
   */
  // TODO(adit): Remove this method. ALLUXIO-2643.
  UnderFileSystemWithLogging(String path, UnderFileSystem ufs, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");
    mPath = path;
    mUnderFileSystem = ufs;
    mConf = conf;
    mEscapedPath = MetricsSystem.escape(new AlluxioURI(path));
    mLoggingThreshold = mConf.getMs(PropertyKey.UNDERFS_LOGGING_THRESHOLD);
  }

  @Override
  public void cleanup() throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.cleanup();
        return null;
      }

      @Override
      public String methodName() {
        return "cleanup";
      }
    });
  }

  @Override
  public void close() throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.close();
        return null;
      }

      @Override
      public String methodName() {
        return "close";
      }
    });
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.connectFromMaster(hostname);
        return null;
      }

      @Override
      public String methodName() {
        return "ConnectFromMaster";
      }

      @Override
      public String toString() {
        return String.format("hostname=%s", hostname);
      }
    });
  }

  @Override
  public void connectFromWorker(final String hostname) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.connectFromWorker(hostname);
        return null;
      }

      @Override
      public String methodName() {
        return "ConnectFromWorker";
      }

      @Override
      public String toString() {
        return String.format("hostname=%s", hostname);
      }
    });
  }

  @Override
  public OutputStream create(final String path) throws IOException {
    return call(new UfsCallable<OutputStream>() {
      @Override
      public OutputStream call() throws IOException {
        return mUnderFileSystem.create(path);
      }

      @Override
      public String methodName() {
        return "Create";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    return call(new UfsCallable<OutputStream>() {
      @Override
      public OutputStream call() throws IOException {
        return mUnderFileSystem.create(path, options);
      }

      @Override
      public String methodName() {
        return "Create";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public OutputStream createNonexistingFile(final String path) throws IOException {
    return call(new UfsCallable<OutputStream>() {
      @Override
      public OutputStream call() throws IOException {
        return mUnderFileSystem.createNonexistingFile(path);
      }

      @Override
      public String methodName() {
        return "CreateNonexistingFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public OutputStream createNonexistingFile(final String path,
      final CreateOptions options) throws IOException {
    return call(new UfsCallable<OutputStream>() {
      @Override
      public OutputStream call() throws IOException {
        return mUnderFileSystem.createNonexistingFile(path, options);
      }

      @Override
      public String methodName() {
        return "CreateNonexistingFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteDirectory(path);
      }

      @Override
      public String methodName() {
        return "DeleteDirectory";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options)
      throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteDirectory(path, options);
      }

      @Override
      public String methodName() {
        return "DeleteDirectory";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean deleteExistingDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteExistingDirectory(path);
      }

      @Override
      public String methodName() {
        return "DeleteExistingDirectory";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean deleteExistingDirectory(final String path, final DeleteOptions options)
      throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteExistingDirectory(path, options);
      }

      @Override
      public String methodName() {
        return "DeleteExistingDirectory";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteFile(path);
      }

      @Override
      public String methodName() {
        return "DeleteFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean deleteExistingFile(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteExistingFile(path);
      }

      @Override
      public String methodName() {
        return "DeleteExistingFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean exists(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.exists(path);
      }

      @Override
      public String methodName() {
        return "Exists";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException, UnimplementedException {
    return call(new UfsCallable<Pair<AccessControlList, DefaultAccessControlList>>() {
      @Override
      public Pair<AccessControlList, DefaultAccessControlList> call() throws IOException {
        return mUnderFileSystem.getAclPair(path);
      }

      @Override
      public String methodName() {
        return "GetAcl";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public long getBlockSizeByte(final String path) throws IOException {
    return call(new UfsCallable<Long>() {
      @Override
      public Long call() throws IOException {
        return mUnderFileSystem.getBlockSizeByte(path);
      }

      @Override
      public String methodName() {
        return "GetBlockSizeByte";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public AlluxioConfiguration getConfiguration() throws IOException {
    return call(new UfsCallable<AlluxioConfiguration>() {
      @Override
      public AlluxioConfiguration call() throws IOException {
        return mUnderFileSystem.getConfiguration();
      }

      @Override
      public String methodName() {
        return "GetConfiguration";
      }

      @Override
      public String toString() {
        return "";
      }
    });
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsDirectoryStatus>() {
      @Override
      public UfsDirectoryStatus call() throws IOException {
        return mUnderFileSystem.getDirectoryStatus(path);
      }

      @Override
      public String methodName() {
        return "GetDirectoryStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsDirectoryStatus>() {
      @Override
      public UfsDirectoryStatus call() throws IOException {
        return mUnderFileSystem.getExistingDirectoryStatus(path);
      }

      @Override
      public String methodName() {
        return "GetExistingDirectoryStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public List<String> getFileLocations(final String path) throws IOException {
    return call(new UfsCallable<List<String>>() {
      @Override
      public List<String> call() throws IOException {
        return mUnderFileSystem.getFileLocations(path);
      }

      @Override
      public String methodName() {
        return "GetFileLocations";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public List<String> getFileLocations(final String path, final FileLocationOptions options)
      throws IOException {
    return call(new UfsCallable<List<String>>() {
      @Override
      public List<String> call() throws IOException {
        return mUnderFileSystem.getFileLocations(path, options);
      }

      @Override
      public String methodName() {
        return "GetFileLocations";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public UfsFileStatus getFileStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsFileStatus>() {
      @Override
      public UfsFileStatus call() throws IOException {
        return mUnderFileSystem.getFileStatus(path);
      }

      @Override
      public String methodName() {
        return "GetFileStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public UfsFileStatus getExistingFileStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsFileStatus>() {
      @Override
      public UfsFileStatus call() throws IOException {
        return mUnderFileSystem.getExistingFileStatus(path);
      }

      @Override
      public String methodName() {
        return "GetExistingFileStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public String getFingerprint(String path) {
    try {
      return call(new UfsCallable<String>() {
        @Override
        public String call() throws IOException {
          return mUnderFileSystem.getFingerprint(path);
        }

        @Override
        public String methodName() {
          return "GetFingerprint";
        }

        @Override
        public String toString() {
          return String.format("path=%s", path);
        }
      });
    } catch (IOException e) {
      // This is not possible.
      return Constants.INVALID_UFS_FINGERPRINT;
    }
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    return mUnderFileSystem.getOperationMode(physicalUfsState);
  }

  @Override
  public long getSpace(final String path, final SpaceType type) throws IOException {
    return call(new UfsCallable<Long>() {
      @Override
      public Long call() throws IOException {
        return mUnderFileSystem.getSpace(path, type);
      }

      @Override
      public String methodName() {
        return "GetSpace";
      }

      @Override
      public String toString() {
        return String.format("path=%s, type=%s", path, type);
      }
    });
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return call(new UfsCallable<UfsStatus>() {
      @Override
      public UfsStatus call() throws IOException {
        return mUnderFileSystem.getStatus(path);
      }

      @Override
      public String methodName() {
        return "GetStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    return call(new UfsCallable<UfsStatus>() {
      @Override
      public UfsStatus call() throws IOException {
        return mUnderFileSystem.getExistingStatus(path);
      }

      @Override
      public String methodName() {
        return "GetExistingStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public String getUnderFSType() {
    return mUnderFileSystem.getUnderFSType();
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.isDirectory(path);
      }

      @Override
      public String methodName() {
        return "IsDirectory";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean isExistingDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.isExistingDirectory(path);
      }

      @Override
      public String methodName() {
        return "IsExistingDirectory";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean isFile(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.isFile(path);
      }

      @Override
      public String methodName() {
        return "IsFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public List<String> getPhysicalStores() {
    return mUnderFileSystem.getPhysicalStores();
  }

  @Override
  public boolean isObjectStorage() {
    return mUnderFileSystem.isObjectStorage();
  }

  @Override
  public UfsStatus[] listStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsStatus[]>() {
      @Override
      public UfsStatus[] call() throws IOException {
        return filterInvalidPaths(mUnderFileSystem.listStatus(path), path);
      }

      @Override
      public String methodName() {
        return "ListStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public UfsStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    return call(new UfsCallable<UfsStatus[]>() {
      @Override
      public UfsStatus[] call() throws IOException {
        return filterInvalidPaths(mUnderFileSystem.listStatus(path, options), path);
      }

      @Override
      public String methodName() {
        return "ListStatus";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Nullable
  private UfsStatus[] filterInvalidPaths(UfsStatus[] statuses, String listedPath) {
    // This is a temporary fix to prevent us from choking on paths containing '?'.
    if (statuses == null) {
      return null;
    }
    int removed = 0;
    for (UfsStatus status : statuses) {
      if (status.getName().contains("?")) {
        LOG.warn("Ignoring {} while listing {} since it contains '?'", status.getName(),
            listedPath);
        removed++;
      }
    }
    if (removed > 0) {
      UfsStatus[] newStatuses = new UfsStatus[statuses.length - removed];
      int i = 0;
      // We perform two passes to keep the common case (no invalid names) very cheap.
      for (UfsStatus status : statuses) {
        if (!status.getName().contains("?")) {
          newStatuses[i++] = status;
        }
      }
      return newStatuses;
    }
    return statuses;
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.mkdirs(path);
      }

      @Override
      public String methodName() {
        return "Mkdirs";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.mkdirs(path, options);
      }

      @Override
      public String methodName() {
        return "Mkdirs";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public InputStream open(final String path) throws IOException {
    return call(new UfsCallable<InputStream>() {
      @Override
      public InputStream call() throws IOException {
        return mUnderFileSystem.open(path);
      }

      @Override
      public String methodName() {
        return "Open";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    return call(new UfsCallable<InputStream>() {
      @Override
      public InputStream call() throws IOException {
        return mUnderFileSystem.open(path, options);
      }

      @Override
      public String methodName() {
        return "Open";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public InputStream openExistingFile(final String path) throws IOException {
    return call(new UfsCallable<InputStream>() {
      @Override
      public InputStream call() throws IOException {
        return mUnderFileSystem.openExistingFile(path);
      }

      @Override
      public String methodName() {
        return "OpenExistingFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s", path);
      }
    });
  }

  @Override
  public InputStream openExistingFile(final String path, final OpenOptions options)
      throws IOException {
    return call(new UfsCallable<InputStream>() {
      @Override
      public InputStream call() throws IOException {
        return mUnderFileSystem.openExistingFile(path, options);
      }

      @Override
      public String methodName() {
        return "OpenExistingFile";
      }

      @Override
      public String toString() {
        return String.format("path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.renameDirectory(src, dst);
      }

      @Override
      public String methodName() {
        return "RenameDirectory";
      }

      @Override
      public String toString() {
        return String.format("src=%s, dst=%s", src, dst);
      }
    });
  }

  @Override
  public boolean renameRenamableDirectory(final String src, final String dst) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.renameRenamableDirectory(src, dst);
      }

      @Override
      public String methodName() {
        return "RenameRenableDirectory";
      }

      @Override
      public String toString() {
        return String.format("src=%s, dst=%s", src, dst);
      }
    });
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.renameFile(src, dst);
      }

      @Override
      public String methodName() {
        return "RenameFile";
      }

      @Override
      public String toString() {
        return String.format("src=%s, dst=%s", src, dst);
      }
    });
  }

  @Override
  public boolean renameRenamableFile(final String src, final String dst) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.renameRenamableFile(src, dst);
      }

      @Override
      public String methodName() {
        return "RenameRenamableFile";
      }

      @Override
      public String toString() {
        return String.format("src=%s, dst=%s", src, dst);
      }
    });
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUnderFileSystem.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.setAclEntries(path, aclEntries);
        return null;
      }

      @Override
      public String methodName() {
        return "SetAclEntries";
      }

      @Override
      public String toString() {
        return String.format("path=%s, ACLEntries=%s", path, aclEntries);
      }
    });
  }

  @Override
  public void setOwner(final String path, final String owner, final String group)
      throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.setOwner(path, owner, group);
        return null;
      }

      @Override
      public String methodName() {
        return "SetOwner";
      }

      @Override
      public String toString() {
        return String.format("path=%s, owner=%s, group=%s", path, owner, group);
      }
    });
  }

  @Override
  public void setMode(final String path, final short mode) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.setMode(path, mode);
        return null;
      }

      @Override
      public String methodName() {
        return "SetMode";
      }

      @Override
      public String toString() {
        return String.format("path=%s, mode=%s", path, mode);
      }
    });
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return mUnderFileSystem.supportsFlush();
  }

  @Override
  public boolean supportsActiveSync() {
    return mUnderFileSystem.supportsActiveSync();
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.startActiveSyncPolling(txId);
      }

      @Override
      public String methodName() {
        return "StartActiveSyncPolling";
      }

      @Override
      public String toString() {
        return String.format("txId=%d", txId);
      }
    });
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.stopActiveSyncPolling();
      }

      @Override
      public String methodName() {
        return "StopActiveSyncPolling";
      }
    });
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    return call(new UfsCallable<SyncInfo>() {
      @Override
      public SyncInfo call() throws IOException {
        return mUnderFileSystem.getActiveSyncInfo();
      }

      @Override
      public String methodName() {
        return "GetActiveSyncInfo";
      }
    });
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.startSync(uri);
        return null;
      }

      @Override
      public String methodName() {
        return "StartSync";
      }

      @Override
      public String toString() {
        return String.format("uri=%s", uri.toString());
      }
    });
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.stopSync(uri);
        return null;
      }

      @Override
      public String methodName() {
        return "StopSync";
      }

      @Override
      public String toString() {
        return String.format("uri=%s", uri.toString());
      }
    });
  }

  /**
   * This is only used in the test.
   *
   * @return the underlying {@link UnderFileSystem}
   */
  public UnderFileSystem getUnderFileSystem() {
    return mUnderFileSystem;
  }

  /**
   * Interface representing a callable to the under storage system which throws an
   * {@link IOException} if an error occurs during the external communication.
   *
   * @param <T> the return type of the callable
   */
  public abstract static class UfsCallable<T> {
    /**
     * Executes the call.
     *
     * @return the result of the call
     */
    abstract T call() throws IOException;

    abstract String methodName();

    @Override
    public String toString() {
      return "";
    }
  }

  /**
   * A wrapper for invoking an {@link UfsCallable} with enter/exit point logging.
   *
   * @param callable the callable to invoke
   * @param <T> the return type
   * @return the result of the callable
   */
  private <T> T call(UfsCallable<T> callable) throws IOException {
    String methodName = callable.methodName();
    long startMs = System.currentTimeMillis();
    long durationMs;
    LOG.debug("Enter: {}({})", methodName, callable);
    try (Timer.Context ctx = MetricsSystem.timer(getQualifiedMetricName(methodName)).time()) {
      T ret = callable.call();
      durationMs = System.currentTimeMillis() - startMs;
      LOG.debug("Exit (OK): {}({}) in {} ms", methodName, callable, durationMs);
      if (durationMs >= mLoggingThreshold) {
        LOG.warn("{}({}) returned OK in {} ms (>={} ms)", methodName,
            callable, durationMs, mLoggingThreshold);
      }
      return ret;
    } catch (IOException e) {
      durationMs = System.currentTimeMillis() - startMs;
      MetricsSystem.counter(getQualifiedFailureMetricName(methodName)).inc();
      LOG.debug("Exit (Error): {}({}) in {} ms, Error={}",
          methodName, callable, durationMs, e.toString());
      if (durationMs >= mLoggingThreshold) {
        LOG.warn("{}({}) returned \"{}\" in {} ms (>={} ms)", methodName,
            callable, e.toString(), durationMs, mLoggingThreshold);
      }
      throw e;
    }
  }

  @Override
  public boolean isSeekable() {
    return mUnderFileSystem.isSeekable();
  }

  // TODO(calvin): General tag logic should be in getMetricName
  private String getQualifiedMetricName(String metricName) {
    try {
      if (SecurityUtils.isAuthenticationEnabled(mConf)
          && AuthenticatedClientUser.get(mConf) != null) {
        return Metric.getMetricNameWithTags(metricName, MetricInfo.TAG_USER,
            AuthenticatedClientUser.get(mConf).getName(), MetricInfo.TAG_UFS,
            mEscapedPath, MetricInfo.TAG_UFS_TYPE,
            mUnderFileSystem.getUnderFSType());
      }
    } catch (IOException e) {
      // fall through
    }
    return Metric.getMetricNameWithTags(metricName, MetricInfo.TAG_UFS,
        mEscapedPath, MetricInfo.TAG_UFS_TYPE,
        mUnderFileSystem.getUnderFSType());
  }

  // TODO(calvin): This should not be in this class
  private String getQualifiedFailureMetricName(String metricName) {
    return getQualifiedMetricName(metricName + "Failures");
  }
}
