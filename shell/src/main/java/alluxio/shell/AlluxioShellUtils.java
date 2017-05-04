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

package alluxio.shell;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.cli.AlluxioShell;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.shell.command.ShellCommand;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.reflections.Reflections;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for convenience methods used by {@link AlluxioShell}.
 */
@ThreadSafe
public final class AlluxioShellUtils {

  private AlluxioShellUtils() {} // prevent instantiation

  /**
   * Removes {@link Constants#HEADER} / {@link Constants#HEADER_FT} and hostname:port information
   * from a path, leaving only the local file path.
   *
   * @param path the path to obtain the local path from
   * @return the local path in string format
   */
  public static String getFilePath(String path) throws IOException {
    path = validatePath(path);
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
   * @return the verified path in a form like alluxio://host:port/dir. If only the "/dir" or "dir"
   *         part is provided, the host and port are retrieved from property,
   *         alluxio.master.hostname and alluxio.master.port, respectively.
   */
  public static String validatePath(String path) throws IOException {
    if (path.startsWith(Constants.HEADER) || path.startsWith(Constants.HEADER_FT)) {
      if (!path.contains(":")) {
        throw new IOException("Invalid Path: " + path + ". Use " + Constants.HEADER
            + "host:port/ ," + Constants.HEADER_FT + "host:port/" + " , or /file");
      } else {
        return path;
      }
    } else {
      String hostname = NetworkAddressUtils.getConnectHost(ServiceType.MASTER_RPC);
      int port =  Configuration.getInt(PropertyKey.MASTER_RPC_PORT);
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
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
    List<AlluxioURI> res = new LinkedList<>();
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
      List<File> res = new LinkedList<>();
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
    List<File> res = new LinkedList<>();
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
   * Gets all supported {@link ShellCommand} classes instances and load them into a map.
   * Provides a way to gain these commands information by their CommandName.
   *
   * @param fileSystem the {@link FileSystem} instance to construct the command
   * @return a mapping from command name to command instance
   */
  public static Map<String, ShellCommand> loadCommands(FileSystem fileSystem) {
    Map<String, ShellCommand> commandsMap = new HashMap<>();
    String pkgName = ShellCommand.class.getPackage().getName();
    Reflections reflections = new Reflections(pkgName);
    for (Class<? extends ShellCommand> cls : reflections.getSubTypesOf(ShellCommand.class)) {
      // Only instantiate a concrete class
      if (!Modifier.isAbstract(cls.getModifiers())) {
        ShellCommand cmd;
        try {
          cmd = CommonUtils.createNewClassInstance(cls, new Class[] {FileSystem.class},
              new Object[] {fileSystem});
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        commandsMap.put(cmd.getCommandName(), cmd);
      }
    }
    return commandsMap;
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
