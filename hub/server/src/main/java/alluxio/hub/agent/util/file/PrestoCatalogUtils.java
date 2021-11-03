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

package alluxio.hub.agent.util.file;

import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.hub.proto.UploadProcessType;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Utilities for interacting with Presto via the hub agents.
 */
public class PrestoCatalogUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PrestoCatalogUtils.class);

  static final String PRESTO_CTL_CMD;

  static {
    String tmpCtl;
    try {
      tmpCtl = new ShellCommand("systemctl --version".split(" "))
          .runWithOutput().getExitCode() == 0 ? "systemctl" : "initctl";
    } catch (IOException e) {
      tmpCtl = "initctl";
    }
    LOG.info("Detected presto ctl type as " + tmpCtl);
    PRESTO_CTL_CMD = tmpCtl;
  }

  /**
   * Get Presto process user and group.
   * @return a pair indicating presto process user and group
   */
  public static Pair<String, String> getPrestoUser() {
    String user = findUser("PrestoServer");
    String group =  findGroup(user);
    LOG.info("Detected presto user as " + user + " group " + group);
    return new Pair<>(user, group);
  }

  /**
   * Get user info based on process type.
   *
   * @param type Upload process type
   * @return user information and group info
   */
  public static Pair<String, String> getProcessInfo(UploadProcessType type) {
    if (type.equals(UploadProcessType.ALLUXIO)) {
      return getAlluxioUser();
    } else {
      return getPrestoUser();
    }
  }

  /**
   * Get Alluxio process user and group.
   * @return a pair indicating alluxio process user and group
   */
  public static Pair<String, String> getAlluxioUser() {
    String userId = System.getenv("ALLUXIO_HUB_USER_ID");
    String user = "";
    if (userId != null) {
      user = findUserForId(userId);
    }
    if (user.isEmpty()) {
      user = findUser("AlluxioMaster");
    }
    if (user.isEmpty()) {
      user = findUser("AlluxioWorker");
    }
    if (user.isEmpty()) {
      return new Pair<>("", "");
    }
    String group = findGroup(user);
    LOG.info("Detected alluxio user as " + user + " group " + group);
    return new Pair<>(user, group);
  }

  /**
   * Find the user name for given id.
   *
   * @param id user id
   * @return user name
   */
  public static String findUserForId(String id) {
    String[] command =
        new String[] {"bash", "-c", "getent passwd | awk -F: '$3 == " + id + "{ print $1 }'"};
    try {
      return new ShellCommand(command).runWithOutput().getOutput().trim();
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * Find the user name of a running process.
   *
   * @param programName process name
   * @return user name
   */
  public static String findUser(String programName) {
    String[] command = new String[] {"bash", "-c", "ps -e -o user -o command | grep " + programName
        + "| grep -v grep " + "| awk '{print $1}'"};
    try {
      return new ShellCommand(command).runWithOutput().getOutput().trim();
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * Find the group name of a user.
   * @param userName user name
   * @return group name
   */
  public static String findGroup(String userName) {
    String[] command = new String[] {"bash", "-c", "groups " + userName + " | awk '{print $1}'"};
    try {
      return new ShellCommand(command).runWithOutput().getOutput().trim();
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * Find the primary group name of a user.
   * @param userName user name
   * @return group name
   */
  public static String getPrimaryGroup(String userName) {
    String[] command = new String[] {"id", "-gn"};
    try {
      return new ShellCommand(command).runWithOutput().getOutput().trim();
    } catch (IOException e) {
      return "";
    }
  }

  /**
   * Convert a set of {@link Properties} to a string of XML suitable for consumption by a process in
   * the hadoop ecosystem.
   *
   * @param props to convert, the original object remains unmodified
   * @return a string representing the XML format of the properties provided
   */
  public static String propertiesToXml(Properties props) {
    List<String> lines = new LinkedList<>();
    lines.add("<?xml version=\"1.0\"?>");
    lines.add("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
    lines.add("<configuration>");
    for (String propName : props.stringPropertyNames()) {
      if (!props.getProperty(propName).isEmpty()) {
        lines.add("<property>");
        lines.add("<name>" + propName + "</name>");
        lines.add("<value>" + props.getProperty(propName) + "</value>");
        lines.add("</property>");
      }
    }
    lines.add("</configuration>");
    return String.join("\n", lines);
  }

  /**
   * Restarts presto in AWS EMR environment.
   *
   * Based on the scripts in the integration/emr/ directory
   *
   * @throws IOException if the restart process fails
   */
  public static void restartPrestoEmr() throws IOException {
    ShellCommand stop = new ShellCommand((PRESTO_CTL_CMD + " stop presto-server").split(" "));
    ShellCommand start = new ShellCommand((PRESTO_CTL_CMD + " start presto-server").split(" "));
    CommandReturn stopRes = stop.runWithOutput();
    if (stopRes.getExitCode() != 0) {
      throw new IOException("Presto stop exit code != 0\n"
          + stopRes.getOutput());
    }
    CommandReturn startRes = start.runWithOutput();
    if (startRes.getExitCode() != 0) {
      throw new IOException("Presto start exit code != 0\n"
          + startRes.getOutput());
    }
  }

  /**
   * Restarts presto in google dataproc environment.
   *
   * @throws IOException if the restart command does not exit successfully
   */
  public static void restartPrestoDataproc() throws IOException {
    ShellCommand restart = new ShellCommand("systemctl restart presto".split(" "));
    CommandReturn res = restart.runWithOutput();
    int code = res.getExitCode();
    if (code != 0) {
      throw new IOException("Failed to restart presto on dataproc, exit code != 0\n"
          + res.getOutput());
    }
  }

  /**
   * Gets the set of files pointed at by hive.config.resources in the file, and returns an array of
   * those files.
   *
   * @param catalogFile the path to the catalog file
   * @return an array representing the files pointed to by the property hive.config.resources
   */
  public static List<Path> getCatalogAlluxioXmlConfigPath(Path catalogFile) throws IOException {
    Properties p = new Properties();
    try (BufferedReader r = Files.newBufferedReader(catalogFile)) {
      p.load(r);
      if (!p.containsKey("hive.config.resources")) {
        return Collections.emptyList();
      }
      String val = p.getProperty("hive.config.resources");
      return Arrays.stream(val.split(","))
          .map(Paths::get)
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.warn("Failed to get the list of presto config files", e);
      throw e;
    }
  }

  private static final CharSequence[] INV_CATALOG_SEQ = {"/", ".."};

  /**
   * Validates that a catalog name does not contain any invalid characters.
   *
   * @param catalogName string representing the name of the catalog to validate
   * @throws AlluxioException if the name is invalid
   */
  public static void validateCatalogName(String catalogName) throws AlluxioException {
    validateAssetName(catalogName, "catalog", Arrays.asList(INV_CATALOG_SEQ));
  }

  /**
   * Validate a particular string representing an asset (catalog, file, etc) does not contain any
   * of a set of invalid character sequences.
   *
   * @param assetName the asset string being validated
   * @param assetType the type of asset that is being validated. This is used in the error
   *                  message when validation is not successful
   * @param invalidSet the set of invalid character sequences which are disallowed in the assetName
   * @throws AlluxioException when validation fails
   */
  public static void validateAssetName(String assetName, String assetType,
      Collection<CharSequence> invalidSet) throws AlluxioException {
    if (invalidSet.stream().anyMatch(assetName::contains)) {
      throw new AlluxioException(String.format(
          "%s name %s may not contain any of the character sequences: %s",
          assetType, assetName, invalidSet));
    }
  }
}
