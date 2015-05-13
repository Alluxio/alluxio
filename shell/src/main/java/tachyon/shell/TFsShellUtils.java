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

package tachyon.shell;

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Class for convenience methods used by {@link TFsShell}.
 */
public class TFsShellUtils {
  /**
   * Removes Constants.HEADER / Constants.HEADER_FT and hostname:port information from a path,
   * leaving only the local file path.
   *
   * @param path The path to obtain the local path from
   * @param tachyonConf The instance of {@link tachyon.conf.TachyonConf} to be used.
   * @return The local path in string format
   * @throws IOException
   */
  public static String getFilePath(String path, TachyonConf tachyonConf) throws IOException {
    path = validatePath(path, tachyonConf);
    if (path.startsWith(Constants.HEADER)) {
      path = path.substring(Constants.HEADER.length());
    } else if (path.startsWith(Constants.HEADER_FT)) {
      path = path.substring(Constants.HEADER_FT.length());
    }
    String ret = path.substring(path.indexOf(TachyonURI.SEPARATOR));
    return ret;
  }

  /**
   * Validates the path, verifying that it contains the <code>Constants.HEADER </code> or
   * <code>Constants.HEADER_FT</code> and a hostname:port specified.
   *
   * @param path The path to be verified.
   * @param tachyonConf The instance of {@link tachyon.conf.TachyonConf} to be used.
   * @return the verified path in a form like tachyon://host:port/dir. If only the "/dir" or "dir"
   *         part is provided, the host and port are retrieved from property,
   *         tachyon.master.hostname and tachyon.master.port, respectively.
   * @throws IOException if the given path is not valid.
   */
  public static String validatePath(String path, TachyonConf tachyonConf) throws IOException {
    if (path.startsWith(Constants.HEADER) || path.startsWith(Constants.HEADER_FT)) {
      if (!path.contains(":")) {
        throw new IOException("Invalid Path: " + path + ". Use " + Constants.HEADER
            + "host:port/ ," + Constants.HEADER_FT + "host:port/" + " , or /file");
      } else {
        return path;
      }
    } else {
      String hostname = tachyonConf.get(Constants.MASTER_HOSTNAME, "localhost");
      int port =  tachyonConf.getInt(Constants.MASTER_PORT, Constants.DEFAULT_MASTER_PORT);
      if (tachyonConf.getBoolean(Constants.USE_ZOOKEEPER, false)) {
        return CommonUtils.concatPath(Constants.HEADER_FT + hostname + ":" + port, path);
      }
      return CommonUtils.concatPath(Constants.HEADER + hostname + ":" + port, path);
    }
  }
}
