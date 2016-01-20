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

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;

/**
 * Class for convenience methods used by {@link TfsShell}.
 */
public class TfsShellUtils {
  /**
   * Removes {@link Constants#HEADER} / {@link Constants#HEADER_FT} and hostname:port information
   * from a path, leaving only the local file path.
   *
   * @param path the path to obtain the local path from
   * @param tachyonConf the instance of {@link tachyon.conf.TachyonConf} to be used
   * @return The local path in string format
   * @throws IOException if the given path is not valid
   */
  public static String getFilePath(String path, TachyonConf tachyonConf) throws IOException {
    path = validatePath(path, tachyonConf);
    if (path.startsWith(Constants.HEADER)) {
      path = path.substring(Constants.HEADER.length());
    } else if (path.startsWith(Constants.HEADER_FT)) {
      path = path.substring(Constants.HEADER_FT.length());
    }
    return path.substring(path.indexOf(TachyonURI.SEPARATOR));
  }

  /**
   * Validates the path, verifying that it contains the {@link Constants#HEADER} or
   * {@link Constants#HEADER_FT} and a hostname:port specified.
   *
   * @param path the path to be verified
   * @param tachyonConf the instance of {@link tachyon.conf.TachyonConf} to be used
   * @return the verified path in a form like tachyon://host:port/dir. If only the "/dir" or "dir"
   *         part is provided, the host and port are retrieved from property,
   *         tachyon.master.hostname and tachyon.master.port, respectively.
   * @throws IOException if the given path is not valid
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
      String hostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, tachyonConf);
      int port =  tachyonConf.getInt(Constants.MASTER_RPC_PORT);
      if (tachyonConf.getBoolean(Constants.ZOOKEEPER_ENABLED)) {
        return PathUtils.concatPath(Constants.HEADER_FT + hostname + ":" + port, path);
      }
      return PathUtils.concatPath(Constants.HEADER + hostname + ":" + port, path);
    }
  }

  /**
   * Get all the {@link TachyonURI}s that match inputURI. If the path is a regular path, the
   * returned list only contains the corresponding URI; Else if the path contains wildcards, the
   * returned list contains all the matched URIs It supports any number of wildcards in inputURI
   *
   * @param tachyonClient the client used to fetch information of Tachyon files
   * @param inputURI the input URI (could contain wildcards)
   * @return a list of {@link TachyonURI}s that matches the inputURI
   * @throws IOException if any filesystem errors are encountered when expanding paths with
   *                     wildcards
   */
  public static List<TachyonURI> getTachyonURIs(TachyonFileSystem tachyonClient,
      TachyonURI inputURI) throws IOException {
    if (!inputURI.getPath().contains(TachyonURI.WILDCARD)) {
      return Lists.newArrayList(inputURI);
    } else {
      String inputPath = inputURI.getPath();
      TachyonURI parentURI =
          new TachyonURI(inputURI.getScheme(), inputURI.getAuthority(),
              inputPath.substring(0, inputPath.indexOf(TachyonURI.WILDCARD) + 1)).getParent();
      return getTachyonURIs(tachyonClient, inputURI, parentURI);
    }
  }

  /**
   * The utility function used to implement getTachyonURIs
   * Basically, it recursively iterates through the directory from the parent directory of inputURI
   * (e.g., for input "/a/b/*", it will start from "/a/b") until it finds all the matches;
   * It does not go into a directory if the prefix mismatches
   * (e.g., for input "/a/b/*", it won't go inside directory "/a/c")
   * @param tachyonClient the client used to fetch metadata of Tachyon files
   * @param inputURI the input URI (could contain wildcards)
   * @param parentDir the {@link TachyonURI} of the directory in which we are searching matched
   *                  files
   * @return a list of {@link TachyonURI}s of the files that match the inputURI in parentDir
   * @throws IOException if any filesystem errors are encountered when expanding paths with
   *                     wildcards
   */
  private static List<TachyonURI> getTachyonURIs(TachyonFileSystem tachyonClient,
      TachyonURI inputURI, TachyonURI parentDir) throws IOException {
    List<TachyonURI> res = new LinkedList<TachyonURI>();
    List<FileInfo> files = null;
    try {
      TachyonFile parentFile = tachyonClient.open(parentDir);
      files = tachyonClient.listStatus(parentFile);
    } catch (TachyonException e) {
      throw new IOException(e);
    }
    for (FileInfo file : files) {
      TachyonURI fileURI =
          new TachyonURI(inputURI.getScheme(), inputURI.getAuthority(), file.getPath());
      if (match(fileURI, inputURI)) { // if it matches
        res.add(fileURI);
      } else {
        if (file.isFolder()) { // if it is a folder, we do it recursively
          TachyonURI dirURI =
              new TachyonURI(inputURI.getScheme(), inputURI.getAuthority(), file.getPath());
          String prefix = inputURI.getLeadingPath(dirURI.getDepth());
          if (prefix != null && match(dirURI, new TachyonURI(prefix))) {
            res.addAll(getTachyonURIs(tachyonClient, inputURI, dirURI));
          }
        }
      }
    }
    return res;
  }

  /**
   * Get the Files (on the local filesystem) that matches input path.
   * If the path is a regular path, the returned list only contains the corresponding file;
   * Else if the path contains wildcards, the returned list contains all the matched Files.
   *
   * @param inputPath The input file path (could contain wildcards)
   * @return a list of files that matches inputPath
   */
  public static List<File> getFiles(String inputPath) {
    File file = new File(inputPath);
    if (!inputPath.contains("*")) {
      List<File> res = new LinkedList<File>();
      if (file.exists()) {
        res.add(file);
      }
      return res;
    } else {
      String prefix = inputPath.substring(0, inputPath.indexOf(TachyonURI.WILDCARD) + 1);
      String parent = new File(prefix).getParent();
      return getFiles(inputPath, parent);
    }
  }

  /**
   * The utility function used to implement getFiles.
   * It follows the same algorithm as {@link #getTachyonURIs}.
   *
   * @param inputPath the input file path (could contain wildcards)
   * @param parent the directory in which we are searching matched files
   * @return a list of files that matches the input path in the parent directory
   */
  private static List<File> getFiles(String inputPath, String parent) {
    List<File> res = new LinkedList<File>();
    File pFile = new File(parent);
    if (!pFile.exists() || !pFile.isDirectory()) {
      return res;
    }
    if (pFile.isDirectory() && pFile.canRead()) {
      for (File file : pFile.listFiles()) {
        if (match(file.getPath(), inputPath)) { // if it matches
          res.add(file);
        } else {
          if (file.isDirectory()) { // if it is a folder, we do it recursively
            TachyonURI dirURI = new TachyonURI(file.getPath());
            String prefix = new TachyonURI(inputPath).getLeadingPath(dirURI.getDepth());
            if (prefix != null && match(dirURI, new TachyonURI(prefix))) {
              res.addAll(getFiles(inputPath, dirURI.getPath()));
            }
          }
        }
      }
    }
    return res;
  }

  /**
   * The characters that have special regex semantics.
   */
  private static final Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\].+*?^$\\\\|]");

  /**
   * Escapes the special characters in a given string.
   *
   * @param str input string
   * @return the string with special characters escaped
   */
  private static String escape(String str) {
    return SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\$0");
  }

  /**
   * Replaces the wildcards with Java's regex semantics.
   */
  private static String replaceWildcards(String text) {
    return escape(text).replace("\\*", ".*");
  }

  /**
   * Returns whether or not fileURI matches the patternURI.
   *
   * @param fileURI the {@link TachyonURI} of a particular file
   * @param patternURI the URI that can contain wildcards
   * @return true if matches; false if not
   */
  private static boolean match(TachyonURI fileURI, TachyonURI patternURI) {
    return escape(fileURI.getPath()).matches(replaceWildcards(patternURI.getPath()));
  }

  /**
   * Returns whether or not filePath matches patternPath.
   *
   * @param filePath path of a given file
   * @param patternPath path that can contain wildcards
   * @return true if matches; false if not
   */
  protected static boolean match(String filePath, String patternPath) {
    return match(new TachyonURI(filePath), new TachyonURI(patternPath));
  }
}
