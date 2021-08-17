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

package alluxio.cli.fs;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by {@link FileSystemShell}.
 */
@ThreadSafe
public final class FileSystemShellUtils {

  private FileSystemShellUtils() {} // prevent instantiation

  /**
   * Removes {@link Constants#HEADER} and hostname:port information
   * from a path, leaving only the local file path.
   *
   * @param path the path to obtain the local path from
   * @param alluxioConf Alluxio configuration
   * @return the local path in string format
   */
  public static String getFilePath(String path, AlluxioConfiguration alluxioConf)
      throws IOException {
    path = validatePath(path, alluxioConf);
    if (path.startsWith(Constants.HEADER)) {
      path = path.substring(Constants.HEADER.length());
    }
    return path.substring(path.indexOf(AlluxioURI.SEPARATOR));
  }

  /**
   * Validates the path, verifying that it contains the {@link Constants#HEADER} and a
   * hostname:port specified.
   *
   * @param path the path to be verified
   * @param alluxioConf Alluxio configuration
   * @return the verified path in a form like alluxio://host:port/dir. If only the "/dir" or "dir"
   *         part is provided, the host and port are retrieved from property,
   *         alluxio.master.hostname and alluxio.master.rpc.port, respectively.
   */
  public static String validatePath(String path, AlluxioConfiguration alluxioConf)
      throws IOException {
    if (path.startsWith(Constants.HEADER)) {
      if (!path.contains(":")) {
        throw new IOException("Invalid Path: " + path + ". Use " + Constants.HEADER
            + "host:port/ , or /file");
      } else {
        return path;
      }
    } else {
      String hostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC, alluxioConf);
      int port = alluxioConf.getInt(PropertyKey.MASTER_RPC_PORT);
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
   */
  private static List<AlluxioURI> getAlluxioURIs(FileSystem alluxioClient, AlluxioURI inputURI,
      AlluxioURI parentDir) throws IOException {
    List<AlluxioURI> res = new ArrayList<>();
    List<URIStatus> statuses;
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
      List<File> res = new ArrayList<>();
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
   *
   * It follows the same algorithm as {@link #getAlluxioURIs}.
   *
   * @param inputPath the input file path (could contain wildcards)
   * @param parent the directory in which we are searching matched files
   * @return a list of files that matches the input path in the parent directory
   */
  private static List<File> getFiles(String inputPath, String parent) {
    List<File> res = new ArrayList<>();
    File pFile = new File(parent);
    if (!pFile.exists() || !pFile.isDirectory()) {
      return res;
    }
    if (pFile.isDirectory() && pFile.canRead()) {
      File[] fileList = pFile.listFiles();
      if (fileList == null) {
        return res;
      }
      for (File file : fileList) {
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
   * Gets the value of an option from the command line.
   *
   * @param cl command line object
   * @param option the option to check for in the command line
   * @param defaultValue default value for the option
   * @return argument from command line or default if not present
   */
  public static boolean getBoolArg(CommandLine cl, Option option, boolean defaultValue) {
    boolean arg = defaultValue;
    if (cl.hasOption(option.getLongOpt())) {
      String argOption = cl.getOptionValue(option.getLongOpt());
      arg = Boolean.parseBoolean(argOption);
    }
    return arg;
  }

  /**
   * Gets the value of an option from the command line.
   *
   * @param cl command line object
   * @param option the option to check for in the command line
   * @param defaultValue default value for the option
   * @return argument from command line or default if not present
   */
  public static int getIntArg(CommandLine cl, Option option, int defaultValue) {
    int arg = defaultValue;
    if (cl.hasOption(option.getLongOpt())) {
      String argOption = cl.getOptionValue(option.getLongOpt());
      arg = Integer.parseInt(argOption);
    }
    return arg;
  }

  /**
   * Gets the value of an option from the command line.
   *
   * @param cl command line object
   * @param option the option to check for in the command line
   * @param defaultValue default value for the option
   * @return argument from command line or default if not present
   */
  public static long getMsArg(CommandLine cl, Option option, long defaultValue) {
    long arg = defaultValue;
    if (cl.hasOption(option.getLongOpt())) {
      String argOption = cl.getOptionValue(option.getLongOpt());
      arg = FormatUtils.parseTimeSize(argOption);
    }
    return arg;
  }

  /**
   * Gets all {@link Command} instances in the same package as {@link FileSystemShell} and load them
   * into a map. Provides a way to gain these commands information by their CommandName.
   *
   * @param fsContext the {@link FileSystemContext} instance to construct the command
   * @return a mapping from command name to command instance
   */
  public static Map<String, Command> loadCommands(FileSystemContext fsContext) {
    return CommandUtils.loadCommands(FileSystemShell.class.getPackage().getName(),
        new Class[] {FileSystemContext.class}, new Object[] {fsContext});
  }

  /**
   * Converts the input time into millisecond unit.
   *
   * @param time the time to be converted into milliseconds
   * @return the time in millisecond unit
   */
  public static long getMs(String time) {
    try {
      return FormatUtils.parseTimeSize(time);
    } catch (Exception e) {
      throw new RuntimeException(ExceptionMessage.INVALID_TIME.getMessage(time));
    }
  }

  /**
   * Escapes the special characters in a given string.
   *
   * @param str input string
   * @return the string with special characters escaped
   */
  private static String escape(String str) {
    return str.replace(".", "%2E")
        .replace("+", "%2B")
        .replace("^", "%5E")
        .replace("$", "%24")
        .replace("*", "%2A");
  }

  /**
   * Replaces the wildcards with Java's regex semantics.
   */
  private static String replaceWildcards(String text) {
    return escape(text).replace("%2A", ".*");
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
  public static boolean match(String filePath, String patternPath) {
    return match(new AlluxioURI(filePath), new AlluxioURI(patternPath));
  }
}
