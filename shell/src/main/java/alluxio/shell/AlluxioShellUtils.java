/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by {@link AlluxioShell}.
 */
@ThreadSafe
public class AlluxioShellUtils {
  /**
   * Removes {@link Constants#HEADER} / {@link Constants#HEADER_FT} and hostname:port information
   * from a path, leaving only the local file path.
   *
   * @param path the path to obtain the local path from
   * @param configuration the instance of {@link Configuration} to be used
   * @return The local path in string format
   * @throws IOException if the given path is not valid
   */
  public static String getFilePath(String path, Configuration configuration) throws IOException {
    path = validatePath(path, configuration);
    if (path.startsWith(Constants.HEADER)) {
      path = path.substring(Constants.HEADER.length());
    } else if (path.startsWith(Constants.HEADER_FT)) {
      path = path.substring(Constants.HEADER_FT.length());
    }
    return path.substring(path.indexOf(AlluxioURI.SEPARATOR));
  }

  /**
   * Validates the path, verifying that it contains the {@link Constants#HEADER} or
   * {@link Constants#HEADER_FT} and a hostname:port specified.
   *
   * @param path the path to be verified
   * @param configuration the instance of {@link Configuration} to be used
   * @return the verified path in a form like alluxio://host:port/dir. If only the "/dir" or "dir"
   *         part is provided, the host and port are retrieved from property,
   *         alluxio.master.hostname and alluxio.master.port, respectively.
   * @throws IOException if the given path is not valid
   */
  public static String validatePath(String path, Configuration configuration) throws IOException {
    if (path.startsWith(Constants.HEADER) || path.startsWith(Constants.HEADER_FT)) {
      if (!path.contains(":")) {
        throw new IOException("Invalid Path: " + path + ". Use " + Constants.HEADER
            + "host:port/ ," + Constants.HEADER_FT + "host:port/" + " , or /file");
      } else {
        return path;
      }
    } else {
      String hostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, configuration);
      int port =  configuration.getInt(Constants.MASTER_RPC_PORT);
      if (configuration.getBoolean(Constants.ZOOKEEPER_ENABLED)) {
        return PathUtils.concatPath(Constants.HEADER_FT + hostname + ":" + port, path);
      }
      return PathUtils.concatPath(Constants.HEADER + hostname + ":" + port, path);
    }
  }

  /**
   * Gets all the {@link AlluxioURI}s that match inputURI. If the path is a regular path, the
   * returned list only contains the corresponding URI; Else if the path contains wildcards, the
   * returned list contains all the matched URIs It supports any number of wildcards in inputURI
   *
   * @param alluxioClient the client used to fetch information of Alluxio files
   * @param inputURI the input URI (could contain wildcards)
   * @return a list of {@link AlluxioURI}s that matches the inputURI
   * @throws IOException if any filesystem errors are encountered when expanding paths with
   *                     wildcards
   */
  public static List<AlluxioURI> getAlluxioURIs(FileSystem alluxioClient, AlluxioURI inputURI)
      throws IOException {
    if (!inputURI.getPath().contains(AlluxioURI.WILDCARD)) {
      return Lists.newArrayList(inputURI);
    } else {
      String inputPath = inputURI.getPath();
      AlluxioURI parentURI = new AlluxioURI(inputURI.getScheme(), inputURI.getAuthority(),
          inputPath.substring(0, inputPath.indexOf(AlluxioURI.WILDCARD) + 1),
          inputURI.getQueryMap()).getParent();
      return getAlluxioURIs(alluxioClient, inputURI, parentURI);
    }
  }

  /**
   * The utility function used to implement getAlluxioURIs.
   *
   * Basically, it recursively iterates through the directory from the parent directory of inputURI
   * (e.g., for input "/a/b/*", it will start from "/a/b") until it finds all the matches;
   * It does not go into a directory if the prefix mismatches
   * (e.g., for input "/a/b/*", it won't go inside directory "/a/c")
   *
   * @param alluxioClient the client used to fetch metadata of Alluxio files
   * @param inputURI the input URI (could contain wildcards)
   * @param parentDir the {@link AlluxioURI} of the directory in which we are searching matched
   *                  files
   * @return a list of {@link AlluxioURI}s of the files that match the inputURI in parentDir
   * @throws IOException if any filesystem errors are encountered when expanding paths with
   *                     wildcards
   */
  private static List<AlluxioURI> getAlluxioURIs(FileSystem alluxioClient, AlluxioURI inputURI,
      AlluxioURI parentDir) throws IOException {
    List<AlluxioURI> res = new LinkedList<AlluxioURI>();
    List<URIStatus> statuses = null;
    try {
      statuses = alluxioClient.listStatus(parentDir);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    for (URIStatus status : statuses) {
      AlluxioURI fileURI =
          new AlluxioURI(inputURI.getScheme(), inputURI.getAuthority(), status.getPath());
      if (match(fileURI, inputURI)) { // if it matches
        res.add(fileURI);
      } else {
        if (status.isFolder()) { // if it is a folder, we do it recursively
          AlluxioURI dirURI =
              new AlluxioURI(inputURI.getScheme(), inputURI.getAuthority(), status.getPath());
          String prefix = inputURI.getLeadingPath(dirURI.getDepth());
          if (prefix != null && match(dirURI, new AlluxioURI(prefix))) {
            res.addAll(getAlluxioURIs(alluxioClient, inputURI, dirURI));
          }
        }
      }
    }
    return res;
  }

  /**
   * Gets the files (on the local filesystem) that match the given input path.
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
      String prefix = inputPath.substring(0, inputPath.indexOf(AlluxioURI.WILDCARD) + 1);
      String parent = new File(prefix).getParent();
      return getFiles(inputPath, parent);
    }
  }

  /**
   * The utility function used to implement getFiles.
   * It follows the same algorithm as {@link #getAlluxioURIs}.
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
            AlluxioURI dirURI = new AlluxioURI(file.getPath());
            String prefix = new AlluxioURI(inputPath).getLeadingPath(dirURI.getDepth());
            if (prefix != null && match(dirURI, new AlluxioURI(prefix))) {
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
   * @param fileURI the {@link AlluxioURI} of a particular file
   * @param patternURI the URI that can contain wildcards
   * @return true if matches; false if not
   */
  private static boolean match(AlluxioURI fileURI, AlluxioURI patternURI) {
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
    return match(new AlluxioURI(filePath), new AlluxioURI(patternPath));
  }
}
