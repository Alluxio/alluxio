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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

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
   * Retrieves the primary gid of the given user.
   *
   * @param userName the user name
   * @return gid
   */
  public static long getGid(String userName) {
    return getIdInfo("-g", userName);
  }

  /**
   * Gets the user name from the user id.
   *
   * @param uid user id
   * @return user name
   */
  public static String getUserName(long uid) {
    return runCommandAndGetOutputLine("id", "-nu", new Long(uid).toString());
  }

  /**
   * Gets the group name from the user id.
   *
   * @param uid user id
   * @return group name
   */
  public static String getGroupName(long uid) {
    return runCommandAndGetOutputLine("id", "-ng", new Long(uid).toString());
  }

  /**
   * Runs the "id" command with the given options on the passed username.
   *
   * @param option option to pass to id (either -u or -g)
   * @param username the username on which to run the command
   * @return the uid (-u) or gid (-g) of username
   */
  private static long getIdInfo(String option, String username) {
    String output = runCommandAndGetOutputLine("id", option, username);
    return Long.parseLong(output);
  }

  /**
   * Runs the given shell command and returns the single line output in string.
   *
   * @param command the command to run
   * @return the first line of the command output
   */
  private static String runCommandAndGetOutputLine(String... command) {
    BufferedReader br = null;
    String commandLine = StringUtils.join(command, " ");
    try {
      final Process idProc = new ProcessBuilder().command(command).start();
      br = new BufferedReader(new InputStreamReader(idProc.getInputStream()));
      // expect only one line output
      final String out = br.readLine();
      if (idProc.waitFor() == 0) {
        return out;
      } else {

        LOG.error("{} completed with error", commandLine);
      }
    } catch (IOException e) {
      LOG.error("Cannot execute: {}", commandLine, e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting: {}", commandLine, e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Exception while closing Process output reader", e);
        }
      }
    }
    return "";
  }
}
