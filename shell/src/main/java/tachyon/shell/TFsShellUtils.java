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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.util.io.PathUtils;

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
        return PathUtils.concatPath(Constants.HEADER_FT + hostname + ":" + port, path);
      }
      return PathUtils.concatPath(Constants.HEADER + hostname + ":" + port, path);
    }
  }
  
  /**
   * Get the TachyonURI that matches inputURI. 
   * If the path is an absolute path, the returned list only contains the corresponding URI;
   * Else if the path contains wildcards, the returned list contains all the matched URIs
   * @param tachyonClient The client that fetches the paths
   * @param inputURI the input URI (could contain wildcards)
   * @return A list of TachyonURI that matches the inputURI
   * @throws IOException
   */
  public static List<TachyonURI> getTachyonURIs(TachyonFS tachyonClient, TachyonURI inputURI)
      throws IOException {    
    if (inputURI.getPath().contains("*") == false) {
      List<TachyonURI> res = new LinkedList<TachyonURI>();
      if (tachyonClient.getFileId(inputURI) != -1) {
        res.add(inputURI);
      }
      return res;
    } else {
      String inputPath = inputURI.getPath();
      TachyonURI parentURI = 
          new TachyonURI(inputURI.getScheme(), 
                         inputURI.getAuthority(),
                         inputPath.substring(0, inputPath.indexOf('*') + 1)).getParent();
      return getTachyonURIs(tachyonClient, inputURI, parentURI); 
    }
  }
  
  // Utility function for getTachyonURIs
  private static List<TachyonURI> getTachyonURIs(TachyonFS tachyonClient,
      TachyonURI inputURI, TachyonURI parentDir) throws IOException {
    List<TachyonURI> res = new LinkedList<TachyonURI>(); 
    List<ClientFileInfo> files = tachyonClient.listStatus(parentDir);
    for (ClientFileInfo file : files) {
      TachyonURI fileURI = new TachyonURI(inputURI.getScheme(), 
                                          inputURI.getAuthority(), 
                                          file.getPath());
      if (match(fileURI, inputURI)) { //if it matches
        res.add(fileURI);
      } else {
        if (file.isFolder) { //if it is a folder, we do it recursively
          TachyonURI dirURI = new TachyonURI(inputURI.getScheme(), 
                                             inputURI.getAuthority(), 
                                             file.getPath());
          String prefix = inputURI.getPathComponent(dirURI.getDepth());
          if (prefix != null && match(dirURI, new TachyonURI(prefix)) == true) {
            res.addAll(getTachyonURIs(tachyonClient, inputURI, dirURI));
          }
        }
      }
    }
    return res;
  }
  
  /**
   * Get the Files (on the local filesystem) that matches input path. 
   * Else if the path contains wildcards, the returned list contains all the matched Files
   * @param path The input URI (could contain wildcards)
   * @return A list of TachyonURI that matches the input path
   */
  public static List<File> getFiles(String path) {
    File file = new File(path);
    if (path.contains("*") == false) {
      List<File> res = new LinkedList<File>();
      if (file.exists()) {
        res.add(file);
      }
      return res;
    } else {
      String prefix = path.substring(0, path.indexOf('*') + 1);
      String parent = new File(prefix).getParent();
      return getFiles(path, parent);
    }
  }
  
  // Utility function for getFiles
  protected static List<File> getFiles(String inputPath, String parent) {
    List<File> res = new LinkedList<File>();
    File pFile = new File(parent);
    if (pFile.exists() == false || pFile.isDirectory() == false) {
      return res;
    }
    if (pFile.isDirectory() && pFile.canRead()) {
      for (File file : pFile.listFiles()) {
        if (match(file.getPath(), inputPath)) { //if it matches
          res.add(file);
        } else {
          if (file.isDirectory()) { //if it is a folder, we do it recursively
            TachyonURI dirURI = new TachyonURI(file.getPath());
            String prefix = 
                new TachyonURI(inputPath).getPathComponent(dirURI.getDepth());     
            if (prefix != null && match(dirURI, new TachyonURI(prefix)) == true) {
              res.addAll(getFiles(inputPath, dirURI.getPath()));
            }
          }
        }
      }
    }
    return res;
  }
  
  private static final Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\].+*?^$\\\\|]");
  
  /**
   * Replace the special characters in a given string
   * @param str Input string
   * @return the string with special characters replaced
   */
  private static String escape(String str) {
    return SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\$0");
  }
  
  /**
   * Replace the wildcards with Java's regex semantics
   */
  private static String replaceWildcards(String text) {
    return escape(text).replace("\\*", ".*");
  }
  
  /**
   * Return whether or not fileURI matches the inputURI
   * @param fileURI The TachyonURI of a particular file
   * @param inputURI The URI that can contain wildcards
   * @return true if matches; false if not
   */
  protected static boolean match(TachyonURI fileURI, TachyonURI inputURI) {
    return escape(fileURI.getPath()).matches(replaceWildcards(inputURI.getPath()));
  }
  
  /**
   * Return whether or not file path matches the input path
   * @param filePath Path of a given file
   * @param inputPath Path that can contain wildcards
   * @return true if matches; false if not
   */
  protected static boolean match(String filePath, String inputPath) {
    return match(new TachyonURI(filePath), new TachyonURI(inputPath));
  }
}
