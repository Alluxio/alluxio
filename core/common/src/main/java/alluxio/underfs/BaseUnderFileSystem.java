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
import alluxio.file.options.DescendantType;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.RateLimiter;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A base abstract {@link UnderFileSystem}.
 */
@ThreadSafe
public abstract class BaseUnderFileSystem implements UnderFileSystem, UfsClient {
  private static final Logger LOG = LoggerFactory.getLogger(BaseUnderFileSystem.class);
  public static final Pair<AccessControlList, DefaultAccessControlList> EMPTY_ACL =
      new Pair<>(null, null);

  /** The UFS {@link AlluxioURI} used to create this {@link BaseUnderFileSystem}. */
  protected final AlluxioURI mUri;

  /** UFS Configuration options. */
  protected final UnderFileSystemConfiguration mUfsConf;

  private final ExecutorService mAsyncIOExecutor;

  private final RateLimiter mRateLimiter;

  /**
   * Constructs an {@link BaseUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this ufs
   * @param ufsConf UFS configuration
   */
  protected BaseUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    mUri = Preconditions.checkNotNull(uri, "uri");
    mUfsConf = Preconditions.checkNotNull(ufsConf, "ufsConf");
    mAsyncIOExecutor = Executors.newCachedThreadPool(
        ThreadFactoryUtils.build(uri.getPath() + "IOThread", true));
    long rateLimit = mUfsConf.isSet(PropertyKey.MASTER_METADATA_SYNC_UFS_RATE_LIMIT)
        ? mUfsConf.getLong(PropertyKey.MASTER_METADATA_SYNC_UFS_RATE_LIMIT) : 0;
    mRateLimiter = RateLimiter.createRateLimiter(rateLimit);
  }

  @Override
  public void close() throws IOException {
    try (Closer closer = Closer.create()) {
      closer.register(() -> {
        if (mAsyncIOExecutor != null) {
          mAsyncIOExecutor.shutdown();
        }
      });
    }
  }

  @Override
  public RateLimiter getRateLimiter() {
    return mRateLimiter;
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return create(path, CreateOptions.defaults(mUfsConf).setCreateParent(true));
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return deleteDirectory(path, DeleteOptions.defaults());
  }

  @Override
  public boolean exists(String path) throws IOException {
    return isFile(path) || isDirectory(path);
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException {
    return EMPTY_ACL;
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    // Noop here by default
  }

  @Override
  public AlluxioConfiguration getConfiguration() {
    return mUfsConf;
  }

  @Override
  public String getFingerprint(String path) {
    // TODO(yuzhu): include default ACL in the fingerprint
    try {
      UfsStatus status = getStatus(path);
      Pair<AccessControlList, DefaultAccessControlList> aclPair = getAclPair(path);

      if (aclPair == null || aclPair.getFirst() == null || !aclPair.getFirst().hasExtended()) {
        return Fingerprint.create(getUnderFSType(), status).serialize();
      } else {
        return Fingerprint.create(getUnderFSType(), status, null, aclPair.getFirst()).serialize();
      }
    } catch (Exception e) {
      // In certain scenarios, it is expected that the UFS path does not exist.
      LOG.debug("Failed fingerprint. path: {} error: {}", path, e.toString());
      return Constants.INVALID_UFS_FINGERPRINT;
    }
  }

  @Override
  public Fingerprint getParsedFingerprint(String path) {
    return getParsedFingerprint(path, null);
  }

  @Override
  public Fingerprint getParsedFingerprint(String path, @Nullable String contentHash) {
    try {
      UfsStatus status = getStatus(path);
      Pair<AccessControlList, DefaultAccessControlList> aclPair = getAclPair(path);

      if (aclPair == null || aclPair.getFirst() == null || !aclPair.getFirst().hasExtended()) {
        return Fingerprint.create(getUnderFSType(), status, contentHash);
      } else {
        return Fingerprint.create(getUnderFSType(), status, contentHash, aclPair.getFirst());
      }
    } catch (IOException e) {
      return Fingerprint.INVALID_FINGERPRINT;
    }
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    UfsMode ufsMode = physicalUfsState.get(mUri.getRootPath());
    if (ufsMode != null) {
      return ufsMode;
    }
    return UfsMode.READ_WRITE;
  }

  @Override
  public List<String> getPhysicalStores() {
    return new ArrayList<>(Arrays.asList(mUri.getRootPath()));
  }

  @Override
  public boolean isObjectStorage() {
    return false;
  }

  @Override
  public boolean isSeekable() {
    return false;
  }

  @Nullable
  @Override
  public Iterator<UfsStatus> listStatusIterable(
      String path, ListOptions options, String startAfter, int batchSize) throws IOException {
    // Calling this method on non s3 UFS might result in OOM because batch based fetching
    // is not supported and this method essentially fetches all ufs status and converts it to
    // an iterator.
    UfsStatus[] result = listStatus(path, options);
    if (result == null) {
      return null;
    }
    Arrays.sort(result, Comparator.comparing(UfsStatus::getName));
    return Iterators.forArray(result);
  }

  @Override
  public void performListingAsync(
      String path, @Nullable String continuationToken, @Nullable String startAfter,
      DescendantType descendantType, boolean checkStatus, Consumer<UfsLoadResult> onComplete,
      Consumer<Throwable> onError) {
    mAsyncIOExecutor.submit(() -> {
      try {
        UfsStatus baseStatus = null;
        if (checkStatus) {
          try {
            baseStatus = getStatus(path);
            if (baseStatus == null && !isObjectStorage()) {
              onComplete.accept(new UfsLoadResult(Stream.empty(), 0,
                  null, null, false, false, false));
              return;
            }
            if (baseStatus != null && (descendantType == DescendantType.NONE
                || baseStatus.isFile())) {
              onComplete.accept(new UfsLoadResult(Stream.of(baseStatus), 1,
                  null, new AlluxioURI(baseStatus.getName()), false,
                  baseStatus.isFile(), isObjectStorage()));
              return;
            }
          } catch (FileNotFoundException e) {
            // if we are not using object storage we know nothing exists at the path,
            // so just return an empty result
            if (!isObjectStorage()) {
              onComplete.accept(new UfsLoadResult(Stream.empty(), 0,
                  null, null, false, false, false));
              return;
            }
          }
        }
        UfsStatus[] items = listStatus(path, ListOptions.defaults()
            .setRecursive(descendantType == DescendantType.ALL));
        if (items != null) {
          if (descendantType == DescendantType.NONE && items.length > 0) {
            assert isObjectStorage() && this instanceof ObjectUnderFileSystem;
            ObjectUnderFileSystem.ObjectPermissions permissions =
                ((ObjectUnderFileSystem) this).getPermissions();
            items = new UfsStatus[] {
                new UfsDirectoryStatus("", permissions.getOwner(), permissions.getGroup(),
                    permissions.getMode())};
          }
          Arrays.sort(items, Comparator.comparing(UfsStatus::getName));
          for (UfsStatus item: items) {
            // performListingAsync is used by metadata sync v2
            // which expects the name of an item to be a full path
            item.setName(PathUtils.concatPath(path, item.getName()));
          }
        }
        if (items != null && items.length == 0) {
          items = null;
        }
        UfsStatus firstItem = baseStatus != null ? baseStatus
            : items != null ? items[0] : null;
        UfsStatus lastItem = items == null ? firstItem
            : items[items.length - 1];
        Stream<UfsStatus> itemStream = items == null ? Stream.empty() : Arrays.stream(items);
        int itemCount = items == null ? 0 : items.length;
        if (baseStatus != null) {
          itemStream = Stream.concat(Stream.of(baseStatus), itemStream);
          itemCount++;
        }
        onComplete.accept(new UfsLoadResult(itemStream, itemCount,
            null, lastItem == null ? null : new AlluxioURI(lastItem.getName()), false,
            firstItem != null && firstItem.isFile(), isObjectStorage()));
      } catch (Throwable t) {
        onError.accept(t);
      }
    });
  }

  @Override
  @Nullable
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    if (!options.isRecursive()) {
      return listStatus(path);
    }
    path = validatePath(path);
    List<UfsStatus> returnPaths = new ArrayList<>();
    // Each element is a pair of (full path, UfsStatus)
    Queue<Pair<String, UfsStatus>> pathsToProcess = new ArrayDeque<>();
    // We call list initially, so we can return null if the path doesn't denote a directory
    UfsStatus[] statuses = listStatus(path);
    if (statuses == null) {
      return null;
    } else {
      for (UfsStatus status : statuses) {
        pathsToProcess.add(new Pair<>(PathUtils.concatPath(path, status.getName()), status));
      }
    }
    while (!pathsToProcess.isEmpty()) {
      final Pair<String, UfsStatus> pathToProcessPair = pathsToProcess.remove();
      final String pathToProcess = pathToProcessPair.getFirst();
      UfsStatus pathStatus = pathToProcessPair.getSecond();
      int beginIndex = path.endsWith(AlluxioURI.SEPARATOR) ? path.length() : path.length() + 1;
      returnPaths.add(pathStatus.setName(pathToProcess.substring(beginIndex)));

      if (pathStatus.isDirectory()) {
        // Add all of its subpaths
        UfsStatus[] children = listStatus(pathToProcess);
        if (children != null) {
          for (UfsStatus child : children) {
            pathsToProcess.add(
                new Pair<>(PathUtils.concatPath(pathToProcess, child.getName()), child));
          }
        }
      }
    }
    return returnPaths.toArray(new UfsStatus[0]);
  }

  @Override
  public InputStream open(String path) throws IOException {
    return open(path, OpenOptions.defaults());
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return mkdirs(path, MkdirsOptions.defaults(mUfsConf));
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri, PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath),
        false);
  }

  @Override
  public boolean supportsActiveSync() {
    return false;
  }

  @Override
  public SyncInfo getActiveSyncInfo() {
    return SyncInfo.emptyInfo();
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return false;
  }

  @Override
  public boolean stopActiveSyncPolling() {
    return false;
  }

  @Override
  public void startSync(AlluxioURI uri)  { }

  @Override
  public void stopSync(AlluxioURI uri) { }

  /**
   * Clean the path by creating a URI and turning it back to a string.
   *
   * @param path the path to validate
   * @return validated path
   */
  protected static String validatePath(String path) {
    return new AlluxioURI(path).toString();
  }
}
