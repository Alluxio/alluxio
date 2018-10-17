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

import alluxio.util.ShellUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuseUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuseUtils.class);

  private AlluxioFuseUtils() {}

  /**
   * Retrieves the uid of the given user.
   *
   * @param userName the user name
   * @return uid
   */
  public static long getUid(String userName) {
    return getIdInfo("-u", userName);
  }

  /**
   * Retrieves the effective gid of the given user name.
   *
   * @param userName the user name
   * @return gid
   */
  public static long getGid(String userName) {
    return getIdInfo("-g", userName);
  }

  /**
   * Retrieves the gid of the given user name and group name.
   * The group name must be registered and must contain the user to get the gid.
   *
   * @param userName the user name
   * @param groupName the group name
   * @return gid
   */
  public static long getGid(String userName, String groupName) throws IOException {
    String result = ShellUtils.execCommand("id", userName);
    String groups = result.substring(result.indexOf("groups="));
    Pattern pattern = Pattern.compile("([0-9]+)\\(" + groupName + "\\)");
    Matcher matcher = pattern.matcher(groups);
    if (matcher.find()) {
      return Long.valueOf(matcher.group(1));
    } else {
      return -1;
    }
  }

  /**
   * Gets the user name from the user id.
   *
   * @param uid user id
   * @return user name
   */
  public static String getUserName(long uid) throws IOException {
    return ShellUtils.execCommand("id", "-nu", Long.toString(uid)).trim();
  }

  /**
   * Gets the effective group name from the user name.
   *
   * @param userName the user name
   * @return group name
   */
  public static String getGroupName(String userName) throws IOException {
    return ShellUtils.execCommand("id", "-ng", userName).trim();
  }

  /**
   * Gets the group name from the user name and group id.
   * The user must belong to the group to get the group name.
   *
   * @param userName the user name
   * @param gid the group id
   * @return group name
   */
  public static String getGroupName(String userName, long gid) throws IOException {
    String result = ShellUtils.execCommand("id", userName);
    String groups = result.substring(result.indexOf("groups="));
    Pattern pattern = Pattern.compile(String.valueOf(gid) + "\\(([^\\(\\)]+)\\)");
    Matcher matcher = pattern.matcher(groups);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return "";
    }
  }

  /**
   * Runs the "id" command with the given options on the passed username.
   *
   * @param option option to pass to id (either -u or -g)
   * @param username the username on which to run the command
   * @return the uid (-u) or gid (-g) of username
   */
  private static long getIdInfo(String option, String username) {
    String output;
    try {
      output = ShellUtils.execCommand("id", option, username).trim();
    } catch (IOException e) {
      LOG.error("Failed to get id from {} with option {}", username, option);
      return -1;
    }
    return Long.parseLong(output);
  }
}
