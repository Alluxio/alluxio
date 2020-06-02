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
import jline.internal.Nullable;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Task;
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
  public static final String SEPARATOR = ":";
  protected final AlluxioConfiguration mConf;
  protected final String mPath;

  protected Map<String, String> mCoreConf;
  protected Map<String, String> mHdfsConf;
  protected StringBuilder mMsg = new StringBuilder();
  protected StringBuilder mAdvice = new StringBuilder();

  /**
   * Creates a new instance of {@link HdfsConfValidationTask}
   * for validating HDFS configuration.
   * @param conf configuration
   */
  public HdfsConfValidationTask(String path, AlluxioConfiguration conf) {
    mPath = path;
    mConf = conf;
  }

  protected static boolean isHdfsScheme(String path) {
    String scheme = new AlluxioURI(path).getScheme();
    if (scheme == null || !scheme.startsWith("hdfs")) {
      return false;
    }
    return true;
  }

  protected TaskResult loadHdfsConfig() {
    Pair<String, String> clientConfFiles = getHdfsConfPaths();
    String coreConfPath = clientConfFiles.getFirst();
    String hdfsConfPath = clientConfFiles.getSecond();

    TaskResult result;
    try {
      mCoreConf = accessAndParseConf("core-site.xml", coreConfPath);
      mHdfsConf = accessAndParseConf("hdfs-site.xml", hdfsConfPath);
      State state = (mCoreConf != null) && (mHdfsConf != null) ? State.OK : State.FAILED;
      result = new TaskResult(state, mName, mMsg.toString(), mAdvice.toString());
    } catch (IOException e) {
      result = new TaskResult(State.FAILED, mName, mMsg.toString(), mAdvice.toString());
      result.setError(e);
    }
    return result;
  }

  protected Pair<String, String> getHdfsConfPaths() {
    // If ServerConfiguration does not contain the key, then a {@link RuntimeException} will be
    // thrown before calling the {@link String#split} method.
    String[] clientHadoopConfFilePaths =
            mConf.get(PropertyKey.UNDERFS_HDFS_CONFIGURATION).split(SEPARATOR);
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

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    if (!isHdfsScheme(mPath)) {
      mMsg.append(String.format("UFS path %s is not HDFS. Skipping validation for HDFS properties.%n", mPath));
      return new TaskResult(State.SKIPPED, mName, mMsg.toString(), mAdvice.toString());
    }

    TaskResult loadConfig = loadHdfsConfig();
    if (loadConfig.mState != State.OK) {
      // If failed to load config files, abort
      return loadConfig;
    }

    // no conflicts between these two
    return checkConflicts();
  }

  // Verify core-site.xml and hdfs.site.xml has no conflicts
  // mCoreConf and mHdfsConf are verified to be non-null as precondition
  protected TaskResult checkConflicts() {
    State state = State.OK;
    for (String k : mCoreConf.keySet()) {
      if (mHdfsConf.containsKey(k)) {
        String hdfsValue = mHdfsConf.get(k);
        String coreValue = mCoreConf.get(k);
        if (!hdfsValue.equals(coreValue)) {
          state = State.FAILED;
          mMsg.append(String.format("Property %s is %s in core-site.xml and %s in hdfs-site.xml",
                  k, coreValue, hdfsValue));
          mAdvice.append(String.format("Please fix the inconsistency for %s in core-site.xml and hdfs.xml.%n", k));
        }
      }
    }
    return new TaskResult(state, mName, mMsg.toString(), mAdvice.toString());
  }

  @Nullable
  // TODO(jiacheng): maybe pass this exception back to TaskResult somehow
  private Map<String, String> accessAndParseConf(String configName, String path) throws IOException {
    if (path == null || path.isEmpty()) {
      mMsg.append(String.format("%s is not configured in Alluxio property %s%n", configName, PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      mAdvice.append(String.format("Please configure %s in %s%n", configName, PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      return null;
    }
    try {
      PathUtils.getPathComponents(path);
    } catch (InvalidPathException e) {
      mMsg.append(String.format("Invalid path %s in Alluxio property %s.%n", path, PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      mAdvice.append(String.format("Please correct the path for %s in %s%n", configName, PropertyKey.UNDERFS_HDFS_CONFIGURATION));
      throw new IOException(e);
    }
    HadoopConfigurationFileParser parser = new HadoopConfigurationFileParser();
    Map<String, String> properties;
    try {
      properties = parser.parseXmlConfNonNull(path);
      mMsg.append(String.format("Successfully loaded %s. %n", path));
    } catch (ParserConfigurationException e) {
      mMsg.append(String.format("Failed to create instance of DocumentBuilder for file: %s. %s.%n",
              path, e.getMessage()));
      mAdvice.append("Please check your configuration for javax.xml.parsers.DocumentBuilder.%n");
      throw new IOException(e);
    } catch (IOException e) {
      mMsg.append(String.format("Failed to read %s. %s.%n", path, e.getMessage()));
      mAdvice.append(String.format("Please check your %s.%n", path));
      throw new IOException(e);
    } catch (SAXException e) {
      mMsg.append(String.format("Failed to parse %s. %s.%n", path, e.getMessage()));
      mAdvice.append(String.format("Please check your %s.%n", path));
      throw new IOException(e);
    }
    return properties;
  }
}
