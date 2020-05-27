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

package alluxio.cli.validation;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.CurrentUser;
import alluxio.security.User;
import alluxio.security.authentication.ImpersonationAuthenticator;
import alluxio.security.user.UserState;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.util.io.PathUtils;

import alluxio.util.network.NetworkAddressUtils;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.xml.sax.SAXException;

import javax.security.auth.Subject;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract class for validating HDFS-related configurations.
 */
public class HdfsConfValidationTask extends AbstractValidationTask {
  /** Name of the environment variable to store the path to Hadoop config directory. */
  protected static final String HADOOP_CONF_DIR_ENV_VAR = "HADOOP_CONF_DIR";

  protected static final Option HADOOP_CONF_DIR_OPTION =
      Option.builder("hadoopConfDir").required(false).hasArg(true)
      .desc("path to server-side hadoop conf dir").build();

  private final AlluxioConfiguration mConf;
  private Map<String, String> mCoreConf;
  private Map<String, String> mHdfsConf;

  /**
   * Creates a new instance of {@link HdfsConfValidationTask}
   * for validating HDFS configuration.
   * @param conf configuration
   */
  public HdfsConfValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public List<Option> getOptionList() {
    List<Option> opts = new ArrayList<>();
    opts.add(HADOOP_CONF_DIR_OPTION);
    return opts;
  }

  @Override
  public State validate(Map<String, String> optionsMap) {
    if (shouldSkip()) {
      return State.SKIPPED;
    }
    if (!validateHdfsSettingParity(optionsMap)) {
      return State.FAILED;
    }
    return State.OK;
  }

  protected boolean shouldSkip() {
    // TODO(jiacheng): this needs to be redefined since we changed the tests
    // How to find out the nested mount path?
    String scheme = new AlluxioURI(mConf.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS))
            .getScheme();
    if (scheme == null || !scheme.startsWith("hdfs")) {
      System.out.format("Root underFS is not HDFS. Skipping validation for HDFS properties.%n");
      return true;
    }
    return false;
  }

  private Pair<String, String> getHdfsConf() throws InvalidPathException {
    // If ServerConfiguration does not contain the key, then a {@link RuntimeException} will be
    // thrown before calling the {@link String#split} method.
    String[] clientHadoopConfFilePaths =
            mConf.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION).split(":");
    String clientCoreSiteFilePath = null;
    String clientHdfsSiteFilePath = null;
    for (String path : clientHadoopConfFilePaths) {
      String[] pathComponents = PathUtils.getPathComponents(path);
      if (pathComponents.length < 1) {
        continue;
      }
      if (pathComponents[pathComponents.length - 1].equals("core-site.xml")) {
        clientCoreSiteFilePath = path;
      } else if (pathComponents[pathComponents.length - 1].equals("hdfs-site.xml")) {
        clientHdfsSiteFilePath = path;
      }
    }
    return new Pair<>(clientCoreSiteFilePath, clientHdfsSiteFilePath);
  }

  private Pair<String, String> getHdfsConfRe() {
    // If ServerConfiguration does not contain the key, then a {@link RuntimeException} will be
    // thrown before calling the {@link String#split} method.
    String[] clientHadoopConfFilePaths =
            mConf.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION).split(":");
    String clientCoreSiteFilePath = null;
    String clientHdfsSiteFilePath = null;
    for (String path : clientHadoopConfFilePaths) {
      if (path.contains("core-site.xml")) {
        clientCoreSiteFilePath = path;
      } else if (path.contains("hdfs-site.xml")) {
        clientHdfsSiteFilePath = path;
      }
    }
    return new Pair<>(clientCoreSiteFilePath, clientHdfsSiteFilePath);
  }

  private List<TaskResult> validateTasks() {
    // TODO(jiacheng): should skip?

    Pair<String, String> clientConfFiles = getHdfsConfRe();
    List<TaskResult> results = new ArrayList<>();
    String coreConfPath = clientConfFiles.getFirst();
    String hdfsConfPath = clientConfFiles.getSecond();

    Pair<TaskResult, Map<String, String>> coreConf = accessAndParseConf("core-site.xml", coreConfPath);
    if (coreConf.getFirst().mState == State.OK) {
      mCoreConf = coreConf.getSecond();
    }
    Pair<TaskResult, Map<String, String>> hdfsConf = accessAndParseConf("hdfs-site.xml", hdfsConfPath);
    if (hdfsConf.getFirst().mState == State.OK) {
      mHdfsConf = hdfsConf.getSecond();
    }

    // no conflicts between these two
    results.add(checkConflicts());

    results.add(sanityCheck());

    return results;
  }

  // Verify core-site.xml and hdfs.site.xml has no conflicts
  private TaskResult checkConflicts() {
    String taskName = "Conflict check between core-site.xml and hdfs.xml";
    if (mCoreConf == null || mHdfsConf == null) {
      return new TaskResult(State.SKIPPED, taskName, "No conflicts.", "");
    }
    StringBuilder buf = new StringBuilder();
    State state = State.OK;
    for (String k : mCoreConf.keySet()) {
      if (mHdfsConf.containsKey(k)) {
        state = State.FAILED;
        String hdfsValue = mHdfsConf.get(k);
        String coreValue = mCoreConf.get(k);
        if (!hdfsValue.equals(coreValue)) {
          buf.append(String.format("Property %s is %s in core-site.xml and %s in hdfs-site.xml",
                  k, coreValue, hdfsValue));
        }
      }
    }
    return new TaskResult(state, taskName, buf.toString(),
            state == State.OK ? "" : "Please fix the inconsistency.");
  }

  private Pair<TaskResult, Map<String, String>> accessAndParseConf(String configName, String path) {
    String taskName = String.format("Access %s", configName);
    if (path == null || path.isEmpty()) {
      return new Pair<>(new TaskResult(State.FAILED, taskName,
              String.format("%s is not configured in Alluxio property %s", configName, PropertyKey.UNDERFS_HDFS_CONFIGURATION),
              String.format("Please configure %s in %s", configName, PropertyKey.UNDERFS_HDFS_CONFIGURATION)), null);
    }
    try {
      PathUtils.getPathComponents(path);
    } catch (InvalidPathException e) {
      // TODO(jiacheng): should we leave this exception this way?
      return new Pair<>(new TaskResult(State.FAILED, taskName,
              String.format("Invalid path %s in Alluxio property %s", path, PropertyKey.UNDERFS_HDFS_CONFIGURATION),
              String.format("Please correct the path for %s in %s", configName, PropertyKey.UNDERFS_HDFS_CONFIGURATION)), null);
    }
    HadoopConfigurationFileParser parser = new HadoopConfigurationFileParser();
    State state = State.OK;
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();
    Map<String, String> properties = null;
    try {
      properties = parser.parseXmlConfNonNull(path);
    } catch (ParserConfigurationException e) {
      msg.append(String.format("Failed to create instance of DocumentBuilder for file: %s. %s. ",
              path, e.getMessage()));
      advice.append("Please check your configuration for javax.xml.parsers.DocumentBuilder");
    } catch (IOException e) {
      msg.append(String.format("Failed to read %s. %s. ", path, e.getMessage()));
      advice.append(String.format("Please check your %s.", path));
    } catch (SAXException e) {
      msg.append(String.format("Failed to parse %s. %s. ", path, e.getMessage()));
      advice.append(String.format("Please check your %s. ", path));
    }
    return new Pair<>(new TaskResult(state, taskName, msg.toString(), advice.toString()), properties);
  }

  public Subject getSubjectFromUGI(UserGroupInformation ugi)
          throws IOException, InterruptedException {
    return ugi.doAs((PrivilegedExceptionAction<Subject>) () -> {
      AccessControlContext context = AccessController.getContext();
      return Subject.getSubject(context);
    });
  }

  public Subject getHadoopSubject() throws IOException {
    Subject subject = null;
    UserGroupInformation ugi = null;
    try {
      ugi = UserGroupInformation.getCurrentUser();
      subject = getSubjectFromUGI(ugi);
    } catch (Exception e) {
      throw new IOException(
              String.format("Failed to get Hadoop subject for the Alluxio client. ugi: %s", ugi), e);
    }
    if (subject == null) {
      System.out.println("Hadoop subject does not exist. Creating a fresh subject for Alluxio client");
      subject = new Subject(false, new HashSet<>(), new HashSet<>(), new HashSet<>());
    }
    if (subject.getPrincipals(CurrentUser.class).isEmpty() && ugi != null) {
      subject.getPrincipals().add(new CurrentUser(ugi.getShortUserName()));
    }
    return subject;
  }

  private boolean validateHdfsSettingParity(Map<String, String> optionsMap) {
    String serverHadoopConfDirPath;
    if (optionsMap.containsKey(HADOOP_CONF_DIR_OPTION.getOpt())) {
      serverHadoopConfDirPath = optionsMap.get(HADOOP_CONF_DIR_OPTION.getOpt());
    } else {
      serverHadoopConfDirPath = System.getenv(HADOOP_CONF_DIR_ENV_VAR);
    }
    if (serverHadoopConfDirPath == null) {
      System.out.println("Path to server-side hadoop configuration unspecified,"
          + " skipping validation for HDFS properties.");
      return true;
    }
    String serverCoreSiteFilePath = PathUtils.concatPath(serverHadoopConfDirPath, "/core-site.xml");
    String serverHdfsSiteFilePath = PathUtils.concatPath(serverHadoopConfDirPath, "/hdfs-site.xml");

    // Find the client conf from Alluxio configuration
    Pair<String, String> clientConfFilePaths;
    try {
      clientConfFilePaths = getHdfsConf();
    } catch (InvalidPathException e) {
      // TODO(jiacheng): verify this err msg
      System.out.format("Invalid path: %s. Skip HDFS config parity check.%n", e.getMessage());
      return true;
    }
    String clientCoreSiteFilePath = clientConfFilePaths.getFirst();
    String clientHdfsSiteFilePath = clientConfFilePaths.getSecond();
    if (clientCoreSiteFilePath == null || clientCoreSiteFilePath.isEmpty()) {
      System.out.println("Cannot locate the client-side core-site.xml,"
          + " skipping validation for HDFS properties.");
      return true;
    }
    if (clientConfFilePaths.getFirst() == null || clientHdfsSiteFilePath.isEmpty()) {
      System.out.println("Cannot locate the client-side hdfs-site.xml,"
          + " skipping validation for HDFS properties.");
      return true;
    }
    return compareConfigurations(clientCoreSiteFilePath, serverCoreSiteFilePath)
        && compareConfigurations(clientHdfsSiteFilePath, serverHdfsSiteFilePath);
  }

  private boolean compareConfigurations(String clientConfigFilePath, String serverConfigFilePath) {
    HadoopConfigurationFileParser parser = new HadoopConfigurationFileParser();
    Map<String, String> serverSiteProps = parser.parseXmlConfiguration(serverConfigFilePath);
    if (serverSiteProps == null) {
      System.err.format("Failed to parse server-side %s.%n", serverConfigFilePath);
      return false;
    }
    Map<String, String> clientSiteProps = parser.parseXmlConfiguration(clientConfigFilePath);
    if (clientSiteProps == null) {
      System.err.format("Failed to parse client-side %s.%n", clientConfigFilePath);
      return false;
    }
    boolean matches = true;
    for (Map.Entry<String, String> prop : clientSiteProps.entrySet()) {
      if (!serverSiteProps.containsKey(prop.getKey())) {
        matches = false;
        System.err.format("%s is configured in %s, but not configured in %s.%n",
            prop.getKey(), clientConfigFilePath, serverConfigFilePath);
      } else if (!prop.getValue().equals(serverSiteProps.get(prop.getKey()))) {
        matches = false;
        System.err.format("%s is set to %s in %s, but to %s in %s.%n",
            prop.getKey(), prop.getValue(), clientConfigFilePath,
            serverSiteProps.get(prop.getKey()), serverConfigFilePath);
      }
    }
    if (!matches) {
      return false;
    }
    for (Map.Entry<String, String> prop : serverSiteProps.entrySet()) {
      if (!clientSiteProps.containsKey(prop.getKey())) {
        matches = false;
        System.err.format("%s is configured in %s, but not configured in %s.%n",
            prop.getKey(), serverConfigFilePath, clientConfigFilePath);
      } else if (!prop.getValue().equals(clientSiteProps.get(prop.getKey()))) {
        matches = false;
        System.err.format("%s is set to %s in %s, but to %s in %s.%n",
            prop.getKey(), prop.getValue(), prop.getValue(),
            clientSiteProps.get(prop.getKey()), clientConfigFilePath);
      }
    }
    return matches;
  }
}
