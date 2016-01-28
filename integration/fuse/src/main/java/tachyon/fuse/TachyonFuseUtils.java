/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.fuse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;

/**
 * Utility methods for Tachyon-FUSE.
 */
@ThreadSafe
public final class TachyonFuseUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private TachyonFuseUtils() {}

  /**
   * Retrieves the uid and primary gid of the user running Tachyon-FUSE.
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
