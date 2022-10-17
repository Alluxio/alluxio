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

package alluxio.fuse;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.fuse.auth.AuthPolicy;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.utils.Environment;
import alluxio.jnifuse.utils.VersionPreference;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryUtils;
import alluxio.security.authorization.Mode;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuseUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseUtils.class);
  private static final long THRESHOLD = Configuration.global()
      .getMs(PropertyKey.FUSE_LOGGING_THRESHOLD);
  private static final int MAX_ASYNC_RELEASE_WAITTIME_MS = 5000;
  /** Most FileSystems on linux limit the length of file name beyond 255 characters. */
  public static final int MAX_NAME_LENGTH = 255;

  public static final long ID_NOT_SET_VALUE = -1;
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;

  public static final long MODE_NOT_SET_VALUE = -1;

  private AlluxioFuseUtils() {}

  /**
   * Checks the input file length.
   *
   * @param uri the Alluxio URI
   * @return error code if file length is not allowed, 0 otherwise
   */
  public static int checkNameLength(AlluxioURI uri) {
    if (uri.getName().length() > MAX_NAME_LENGTH) {
      LOG.error("Failed to execute on {}: name longer than {} characters",
          uri, MAX_NAME_LENGTH);
      return -ErrorCodes.ENAMETOOLONG();
    }
    return 0;
  }

  /**
   * Creates a file in alluxio namespace.
   *
   * @param fileSystem the file system
   * @param authPolicy the authentication policy
   * @param uri the alluxio uri
   * @param mode the create mode
   * @return a file out stream
   */
  public static FileOutStream createFile(FileSystem fileSystem, AuthPolicy authPolicy,
      AlluxioURI uri, long mode) {
    CreateFilePOptions.Builder optionsBuilder = CreateFilePOptions.newBuilder();
    if (mode != MODE_NOT_SET_VALUE) {
      optionsBuilder.setMode(new Mode((short) mode).toProto());
    }
    try {
      FileOutStream out = fileSystem.createFile(uri,
          optionsBuilder.build());
      authPolicy.setUserGroupIfNeeded(uri);
      return out;
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(String.format(
          "Failed to create file %s [mode: %s, auth policy: %s]",
          uri, mode, authPolicy.getClass().getName()), e);
    }
  }

  /**
   * Deletes a file or a directory in alluxio namespace.
   *
   * @param fileSystem the file system
   * @param uri the alluxio uri
   */
  public static void deletePath(FileSystem fileSystem, AlluxioURI uri) {
    try {
      fileSystem.delete(uri);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(String.format("Failed to delete path %s", uri), e);
    }
  }

  /**
   * Sets attribute for a file.
   *
   * @param fileSystem the file system
   * @param uri the alluxio uri
   * @param options the set attribute options
   */
  public static void setAttribute(FileSystem fileSystem,
      AlluxioURI uri, SetAttributePOptions options) {
    try {
      fileSystem.setAttribute(uri, options);
    } catch (IOException | AlluxioException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the libjnifuse version preference set by user.
   *
   * @param conf the configuration object
   * @return the version preference
   */
  public static VersionPreference getVersionPreference(AlluxioConfiguration conf) {
    if (Environment.isMac()) {
      LOG.info("osxfuse doesn't support libfuse3 api. Using libfuse version 2.");
      return VersionPreference.VERSION_2;
    }

    final int val = conf.getInt(PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION);
    if (val == 2) {
      return VersionPreference.VERSION_2;
    } else if (val == 3) {
      return VersionPreference.VERSION_3;
    } else {
      return VersionPreference.NO;
    }
  }

  /**
   * Tries to laod Alluxio config from Alluxio Master through Grpc.
   *
   * @param fsContext for communicating with master
   *
   * @return the Alluxio config if loaded successfully; the unmodified conf otherwise
   */
  public static AlluxioConfiguration tryLoadingConfigFromMaster(FileSystemContext fsContext) {
    try {
      InetSocketAddress confMasterAddress =
          fsContext.getMasterClientContext().getConfMasterInquireClient().getPrimaryRpcAddress();
      RetryUtils.retry("load cluster default configuration with master " + confMasterAddress,
          () -> fsContext.getClientContext().loadConfIfNotLoaded(confMasterAddress),
          RetryUtils.defaultClientRetry());
    } catch (IOException e) {
      LOG.warn("Failed to load cluster default configuration for Fuse process. "
          + "Proceed with local configuration for FUSE: {}", e.toString());
    }
    return fsContext.getClusterConf();
  }

  /**
   * @return the system uid
   */
  public static long getSystemUid() {
    String launchUser = System.getProperty("user.name");
    if (launchUser == null || launchUser.isEmpty()) {
      throw new RuntimeException("Failed to get current system user name");
    }
    Optional<Long> launchUserId = AlluxioFuseUtils.getUid(launchUser);
    if (!launchUserId.isPresent()) {
      throw new RuntimeException(
          "Failed to get uid of system user "
              + launchUser);
    }
    return launchUserId.get();
  }

  /**
   * @return the system gid
   */
  public static long getSystemGid() {
    String launchUser = System.getProperty("user.name");
    if (launchUser == null || launchUser.isEmpty()) {
      throw new RuntimeException("Failed to get current system user name");
    }
    Optional<String> launchGroupName = AlluxioFuseUtils.getGroupName(launchUser);
    if (!launchGroupName.isPresent()) {
      throw new RuntimeException(
          "Failed to get group name from system user name "
              + launchUser);
    }
    Optional<Long> launchGroupId = AlluxioFuseUtils.getGidFromGroupName(launchGroupName.get());
    if (!launchGroupId.isPresent()) {
      throw new RuntimeException(
          "Failed to get gid of system group "
              + launchGroupName.get());
    }
    return launchGroupId.get();
  }

  /**
   * Retrieves the uid of the given user.
   *
   * @param userName the user name
   * @return uid
   */
  public static Optional<Long> getUid(String userName) {
    return getIdInfo("-u", userName);
  }

  /**
   * Retrieves the primary gid of the given user.
   *
   * @param userName the user name
   * @return gid
   */
  public static Optional<Long> getGidFromUserName(String userName) {
    return getIdInfo("-g", userName);
  }

  /**
   * Retrieves the gid of the given group.
   *
   * @param groupName the group name
   * @return gid
   */
  public static Optional<Long> getGidFromGroupName(String groupName) {
    try {
      if (OSUtils.isLinux()) {
        String script = "getent group " + groupName + " | cut -d: -f3";
        String result = ShellUtils.execCommand("bash", "-c", script).trim();
        return Optional.of(Long.parseLong(result));
      } else if (OSUtils.isMacOS()) {
        String script = "dscl . -read /Groups/" + groupName
            + " | awk '($1 == \"PrimaryGroupID:\") { print $2 }'";
        String result = ShellUtils.execCommand("bash", "-c", script).trim();
        return Optional.of(Long.parseLong(result));
      }
      return Optional.empty();
    } catch (NumberFormatException | IOException e) {
      LOG.error("Failed to get gid from group name {}.", groupName);
      return Optional.empty();
    }
  }

  /**
   * Gets the user name from the user id.
   *
   * @param uid user id
   * @return user name
   */
  public static Optional<String> getUserName(long uid) {
    if (uid == ID_NOT_SET_VALUE || uid == ID_NOT_SET_VALUE_UNSIGNED) {
      return Optional.empty();
    }
    try {
      String userName = ShellUtils.execCommand("bash", "-c", "id -nu " + uid).trim();
      return userName.isEmpty() ? Optional.empty() : Optional.of(userName);
    } catch (IOException e) {
      LOG.error("Failed to get user name of uid {}", uid, e);
      return Optional.empty();
    }
  }

  /**
   * Gets the primary group name from the user name.
   *
   * @param userName the user name
   * @return group name
   */
  public static Optional<String> getGroupName(String userName) {
    try {
      List<String> groups = CommonUtils.getUnixGroups(userName);
      return groups.isEmpty() ? Optional.empty() : Optional.of(groups.get(0));
    } catch (IOException e) {
      LOG.error("Failed to get group name of user name {}", userName, e);
      return Optional.empty();
    }
  }

  /**
   * Gets the group name from the group id.
   *
   * @param gid the group id
   * @return group name
   */
  public static Optional<String> getGroupName(long gid) {
    if (gid == ID_NOT_SET_VALUE || gid == ID_NOT_SET_VALUE_UNSIGNED) {
      return Optional.empty();
    }
    try {
      String groupName = null;
      if (OSUtils.isLinux()) {
        String script = "getent group " + gid + " | cut -d: -f1";
        groupName = ShellUtils.execCommand("bash", "-c", script).trim();
      } else if (OSUtils.isMacOS()) {
        String script =
            "dscl . list /Groups PrimaryGroupID | awk '($2 == \"" + gid + "\") { print $1 }'";
        groupName = ShellUtils.execCommand("bash", "-c", script).trim();
      }
      return groupName == null || groupName.isEmpty() ? Optional.empty()
          : Optional.of(groupName);
    } catch (IOException e) {
      LOG.error("Failed to get group name of gid {}", gid, e);
      return Optional.empty();
    }
  }

  /**
   * Checks whether fuse is installed in local file system. Alluxio-Fuse only support mac and linux.
   *
   * @return true if fuse is installed, false otherwise
   */
  public static boolean isFuseInstalled() {
    try {
      if (OSUtils.isLinux()) {
        String result = ShellUtils.execCommand("fusermount", "-V");
        return !result.isEmpty();
      } else if (OSUtils.isMacOS()) {
        String result = ShellUtils.execCommand("bash", "-c",
            "pkgutil --pkgs | grep -i com.github.osxfuse.pkg.Core");
        return !result.isEmpty();
      }
    } catch (Exception e) {
      return false;
    }
    return false;
  }

  /**
   * Runs the "id" command with the given options on the passed username.
   *
   * @param option option to pass to id (either -u or -g)
   * @param username the username on which to run the command
   * @return the uid (-u) or gid (-g) of username
   */
  private static Optional<Long> getIdInfo(String option, String username) {
    try {
      String output = ShellUtils.execCommand("id", option, username).trim();
      return Optional.of(Long.parseLong(output));
    } catch (IOException | NumberFormatException e) {
      LOG.error("Failed to get id from {} with option {}", username, option);
      return Optional.empty();
    }
  }

  /**
   * Gets the corresponding error code of a throwable.
   *
   * @param t throwable
   * @return the corresponding error code
   */
  public static int getErrorCode(Throwable t) {
    // Error codes and their explanations are described in
    // the Errno.java in jni-constants
    if (t instanceof AlluxioException) {
      return getAlluxioErrorCode((AlluxioException) t);
    } else if (t instanceof IOException) {
      return -ErrorCodes.EIO();
    } else {
      return -ErrorCodes.EBADMSG();
    }
  }

  /**
   * Gets the corresponding error code of an Alluxio exception.
   *
   * @param e an Alluxio exception
   * @return the corresponding error code
   */
  private static int getAlluxioErrorCode(AlluxioException e) {
    try {
      throw e;
    } catch (FileDoesNotExistException ex) {
      return -ErrorCodes.ENOENT();
    } catch (FileAlreadyExistsException ex) {
      return -ErrorCodes.EEXIST();
    } catch (InvalidPathException ex) {
      return -ErrorCodes.EFAULT();
    } catch (BlockDoesNotExistRuntimeException ex) {
      // TODO(jianjian) handle runtime exception for fuse in base class?
      return -ErrorCodes.ENODATA();
    } catch (DirectoryNotEmptyException ex) {
      return -ErrorCodes.ENOTEMPTY();
    } catch (AccessControlException ex) {
      return -ErrorCodes.EACCES();
    } catch (ConnectionFailedException ex) {
      return -ErrorCodes.ECONNREFUSED();
    } catch (FileAlreadyCompletedException ex) {
      return -ErrorCodes.EOPNOTSUPP();
    } catch (AlluxioException ex) {
      return -ErrorCodes.EBADMSG();
    }
  }

  /**
   * Gets the path status.
   *
   * @param fileSystem the file system
   * @param uri the Alluxio uri to get status of
   * @return the file status
   */
  public static Optional<URIStatus> getPathStatus(FileSystem fileSystem, AlluxioURI uri) {
    try {
      return Optional.of(fileSystem.getStatus(uri));
    } catch (InvalidPathException | FileNotFoundException | FileDoesNotExistException e) {
      return Optional.empty();
    } catch (IOException | AlluxioException ex) {
      throw new RuntimeException(String.format("Failed to get path status of %s", uri), ex);
    }
  }

  /**
   * Waits for the file to complete. This method is mainly added to make sure
   * the async release() when writing a file finished before getting status of
   * the file or opening the file for read().
   *
   * @param fileSystem the file system to get file status
   * @param uri the file path to check
   * @return uri status if file is completed and empty otherwise
   */
  public static Optional<URIStatus> waitForFileCompleted(FileSystem fileSystem, AlluxioURI uri) {
    try {
      return Optional.of(CommonUtils.waitForResult("file completed", () -> {
        try {
          return fileSystem.getStatus(uri);
        } catch (Exception e) {
          throw new RuntimeException(
              String.format("Unexpected error while getting backup status: %s", e));
        }
      }, URIStatus::isCompleted,
          WaitForOptions.defaults().setTimeoutMs(MAX_ASYNC_RELEASE_WAITTIME_MS)));
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    } catch (TimeoutException te) {
      return Optional.empty();
    }
  }

  /**
   * An interface representing a callable for FUSE APIs.
   */
  public interface FuseCallable {
    /**
     * The RPC implementation.
     *
     * @return the return value from the RPC
     */
    int call();
  }

  /**
   * Calls the given {@link FuseCallable} and returns its result.
   *
   * @param logger the logger to use for this call
   * @param callable the callable to call
   * @param methodName the name of the method, used for metrics
   * @param description the format string of the description, used for logging
   * @param args the arguments for the description
   * @return the result
   */
  public static int call(Logger logger, FuseCallable callable, String methodName,
      String description, Object... args) {
    int ret;
    try {
      String debugDesc = logger.isDebugEnabled() ? String.format(description, args) : null;
      logger.debug("Enter: {}({})", methodName, debugDesc);
      long startMs = System.currentTimeMillis();
      ret = callable.call();
      long durationMs = System.currentTimeMillis() - startMs;
      logger.debug("Exit ({}): {}({}) in {} ms", ret, methodName, debugDesc, durationMs);
      MetricsSystem.timer(methodName).update(durationMs, TimeUnit.MILLISECONDS);
      MetricsSystem.timer(MetricKey.FUSE_TOTAL_CALLS.getName())
          .update(durationMs, TimeUnit.MILLISECONDS);
      if (ret < 0) {
        MetricsSystem.counter(methodName + "Failures").inc();
      }
      if (durationMs >= THRESHOLD) {
        logger.warn("{}({}) returned {} in {} ms (>={} ms)", methodName,
            String.format(description, args), ret, durationMs, THRESHOLD);
      }
    } catch (Throwable t) {
      // native code cannot deal with any throwable
      // wrap all the logics in try catch
      String errorMessage;
      try {
        errorMessage = String.format(description, args);
      } catch (Throwable inner) {
        errorMessage = "";
      }
      LOG.error("Failed to {}({}) with unexpected throwable: ", methodName, errorMessage, t);
      return -ErrorCodes.EIO();
    }
    return ret;
  }

  /**
   * Gets the path be mounted to local fuse mount point.
   *
   * @param conf the configuration to get path from
   * @return the mounted root path
   */
  public static String getMountedRootPath(AlluxioConfiguration conf) {
    return conf.getBoolean(PropertyKey.USER_UFS_ENABLED)
        ? conf.getString(PropertyKey.USER_UFS_ADDRESS)
        : conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH);
  }

  /**
   * Gets the cache for resolving FUSE path into {@link AlluxioURI}.
   *
   * @param conf the configuration
   * @return the cache
   */
  public static LoadingCache<String, AlluxioURI> getPathResolverCache(AlluxioConfiguration conf) {
    return CacheBuilder.newBuilder()
        .maximumSize(conf.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX))
        .build(new AlluxioFuseUtils.PathCacheLoader(
            new AlluxioURI(getMountedRootPath(conf))));
  }

  /**
   * Resolves a FUSE path into {@link AlluxioURI} and possibly keeps it in the cache.
   */
  static final class PathCacheLoader extends CacheLoader<String, AlluxioURI> {
    private final AlluxioURI mRootURI;

    /**
     * Constructs a new {@link PathCacheLoader}.
     *
     * @param rootURI the root URI
     */
    PathCacheLoader(AlluxioURI rootURI) {
      mRootURI = Preconditions.checkNotNull(rootURI);
    }

    @Override
    public AlluxioURI load(String fusePath) {
      // fusePath is guaranteed to always be an absolute path (i.e., starts
      // with a fwd slash) - relative to the FUSE mount point
      return mRootURI.join(fusePath);
    }
  }
}
