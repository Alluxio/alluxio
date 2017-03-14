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
   * Retrieves the uid and primary gid of the user running Alluxio-FUSE.
   * @return a long[2] array {uid, gid}
   */
  public static long[] getUidAndGid() {
    final String uname = System.getProperty("user.name");
    final long uid = getIdInfo("-u", uname);
    final long gid = getIdInfo("-g", uname);
    return new long[] {uid, gid};
  }

  /**
   * Runs the "id" command with the given options on the passed username.
   * @param option option to pass to id (either -u or -g)
   * @param username the username on which to run the command
   * @return the uid (-u) or gid (-g) of username
   */
  private static long getIdInfo(String option, String username) {
    BufferedReader br = null;
    try {
      final Process idProc = new ProcessBuilder().command("id", option, username).start();
      br = new BufferedReader(new InputStreamReader(idProc.getInputStream()));
      // expect only one line output
      final String out = br.readLine();
      if (idProc.waitFor() == 0) {
        return Long.parseLong(out);
      } else {
        LOG.error("id {} {} completed with error", option, username);
      }
    } catch (IOException e) {
      LOG.error("Cannot execute: id {} {}", option, username, e);
    } catch (InterruptedException e) {
      LOG.error("Interrupted while waiting: id {} {}", option, username, e);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          LOG.warn("Exception while closing Process output reader", e);
        }
      }
    }
    return -1;
  }
}
