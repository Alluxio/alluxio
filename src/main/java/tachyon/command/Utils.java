/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.command;

import java.io.IOException;
import java.net.InetSocketAddress;

import tachyon.Constants;

/**
 * Class for convenience methods used by TFsShell.
 */
public class Utils {
  public static final String HEADER = "tachyon://";

  /**
   * Validates the path, verifying that it contains the header and a hostname:port specified.
   * @param path The path to be verified.
   * @throws IOException 
   */
  public static String validateTachyonPath(String path) throws IOException {
    if (path.startsWith(HEADER) || path.startsWith(Constants.FT_HEADER)) {
      if (!path.contains(":")) {
        throw new IOException("Invalid Path: " + path + "\n Use tachyon://host:port/ "
          + "tachyon_ft://host:port/" + "or /file");
      } else {
        return path;
      }
    } else {
      String HOSTNAME = System.getProperty("tachyon.master.hostname", "localhost");
      String PORT = System.getProperty("tachyon.master.port", "" + Constants.DEFAULT_MASTER_PORT);
      return HEADER + HOSTNAME + ":" + PORT + path;
    }
  }

  /**
   * Removes header and hostname:port information from a path, leaving only the local file path.
   * @param path The path to obtain the local path from
   * @return The local path in string format
   * @throws IOException 
   */ 
  public static String getFilePath(String path) throws IOException {
    path = validateTachyonPath(path);
    if (path.startsWith(HEADER))
      path = path.substring(HEADER.length());
    if (path.startsWith(Constants.FT_HEADER))
      path = path.substring(Constants.FT_HEADER.length());
    String ret = path.substring(path.indexOf("/"));
    return ret;
  }

  /**
   * Obtains the InetSocketAddress from a path by parsing the hostname:port portion of the path.
   * @param path The path to obtain the InetSocketAddress from.
   * @return The InetSocketAddress of the master node.
   * @throws IOException 
   */
  public static InetSocketAddress getTachyonMasterAddress(String path) throws IOException {
    path = validateTachyonPath(path);
    if (path.startsWith(HEADER))
      path = path.substring(HEADER.length());
    if (path.startsWith(Constants.FT_HEADER))
      path = path.substring(Constants.FT_HEADER.length());
    String masterAddress = path.substring(0, path.indexOf("/"));
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new InetSocketAddress(masterHost, masterPort);
  }
  
  /**
   * Obtains the tachyon master full address from a path by parsing the prefiex and hostname:port 
   * portions of the path.
   * @param path The path to obtain the InetSocketAddress from.
   * @return The InetSocketAddress of the master node.
   * @throws IOException 
   */
  public static String getTachyonMasterAddressAsString(String path) throws IOException {
    path = validateTachyonPath(path);
    String prefix = null;
    if(path.startsWith(HEADER)) {
      prefix = HEADER;
    }
    if(path.startsWith(Constants.FT_HEADER)) {
      prefix = Constants.FT_HEADER;
    }
    path = path.substring(prefix.length());
    String masterAddress = path.substring(0, path.indexOf("/"));
    return new String(prefix + masterAddress);
  }
}