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
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistRuntimeException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.jnifuse.utils.Environment;
import alluxio.jnifuse.utils.VersionPreference;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.util.OSUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.serce.jnrfuse.ErrorCodes;

import java.io.IOException;
import java.util.List;
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

  public static final String DEFAULT_USER_NAME = System.getProperty("user.name");
  public static final long DEFAULT_UID = getUid(DEFAULT_USER_NAME);
  public static final String DEFAULT_GROUP_NAME = getGroupName(DEFAULT_USER_NAME);
  public static final long DEFAULT_GID = getGidFromGroupName(DEFAULT_GROUP_NAME);

  public static final String INVALID_USER_GROUP_NAME = "";
  public static final long ID_NOT_SET_VALUE = -1;
  public static final long ID_NOT_SET_VALUE_UNSIGNED = 4294967295L;

  private AlluxioFuseUtils() {}

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
   * Retrieves the uid of the given user.
   *
   * @param userName the user name
   * @return uid or -1 on failures
   */
  public static long getUid(String userName) {
    return getIdInfo("-u", userName);
  }

  /**
   * Retrieves the primary gid of the given user.
   *
   * @param userName the user name
   * @return gid or -1 on failures
   */
  public static long getGid(String userName) {
    return getIdInfo("-g", userName);
  }

  /**
   * Retrieves the gid of the given group.
   *
   * @param groupName the group name
   * @return gid or -1 on failures
   */
  public static long getGidFromGroupName(String groupName) {
    try {
      if (OSUtils.isLinux()) {
        String script = "getent group " + groupName + " | cut -d: -f3";
        String result = ShellUtils.execCommand("bash", "-c", script).trim();
        return Long.parseLong(result);
      } else if (OSUtils.isMacOS()) {
        String script = "dscl . -read /Groups/" + groupName
            + " | awk '($1 == \"PrimaryGroupID:\") { print $2 }'";
        String result = ShellUtils.execCommand("bash", "-c", script).trim();
        return Long.parseLong(result);
      }
      return ID_NOT_SET_VALUE;
    } catch (NumberFormatException | IOException e) {
      LOG.error("Failed to get gid from group name {}.", groupName);
      return ID_NOT_SET_VALUE;
    }
  }

  /**
   * Gets the user name from the user id.
   *
   * @param uid user id
   * @return user name
   */
  public static String getUserName(long uid) {
    try {
      return ShellUtils.execCommand("bash", "-c", "id -nu", Long.toString(uid)).trim();
    } catch (IOException e) {
      LOG.error("Failed to get user name of uid {}", uid, e);
      return INVALID_USER_GROUP_NAME;
    }
  }

  /**
   * Gets the primary group name from the user name.
   *
   * @param userName the user name
   * @return group name
   */
  public static String getGroupName(String userName) {
    try {
      List<String> groups = CommonUtils.getUnixGroups(userName);
      return groups.isEmpty() ? INVALID_USER_GROUP_NAME : groups.get(0);
    } catch (IOException e) {
      LOG.error("Failed to get group name of user name {}", userName, e);
      return INVALID_USER_GROUP_NAME;
    }
  }

  /**
   * Gets the group name from the group id.
   *
   * @param gid the group id
   * @return group name
   */
  public static String getGroupName(long gid) {
    try {
      if (OSUtils.isLinux()) {
        String script = "getent group " + gid + " | cut -d: -f1";
        return ShellUtils.execCommand("bash", "-c", script).trim();
      } else if (OSUtils.isMacOS()) {
        String script =
            "dscl . list /Groups PrimaryGroupID | awk '($2 == \"" + gid + "\") { print $1 }'";
        return ShellUtils.execCommand("bash", "-c", script).trim();
      }
    } catch (IOException e) {
      LOG.error("Failed to get group name of gid {}", gid, e);
      return INVALID_USER_GROUP_NAME;
    }
    return INVALID_USER_GROUP_NAME;
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
  private static long getIdInfo(String option, String username) {
    try {
      String output = ShellUtils.execCommand("id", option, username).trim();
      return Long.parseLong(output);
    } catch (IOException | NumberFormatException e) {
      LOG.error("Failed to get id from {} with option {}", username, option);
      return ID_NOT_SET_VALUE;
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
   * Waits for the file to complete. This method is mainly added to make sure
   * the async release() when writing a file finished before getting status of
   * the file or opening the file for read().
   *
   * @param fileSystem the file system to get file status
   * @param uri the file path to check
   * @return whether the file is completed or not
   */
  public static boolean waitForFileCompleted(FileSystem fileSystem, AlluxioURI uri) {
    try {
      CommonUtils.waitFor("file completed", () -> {
        try {
          return fileSystem.getStatus(uri).isCompleted();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }, WaitForOptions.defaults().setTimeoutMs(MAX_ASYNC_RELEASE_WAITTIME_MS));
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException te) {
      return false;
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
    int ret = -1;
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
      String errorMessage = "";
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
}
