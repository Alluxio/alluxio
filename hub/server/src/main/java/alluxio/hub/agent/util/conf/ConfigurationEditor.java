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

package alluxio.hub.agent.util.conf;

import alluxio.Constants;
import alluxio.hub.proto.AlluxioConfigurationSet;
import alluxio.util.ConfigurationUtils;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.annotation.Nullable;

/**
 * An object which allows the reading and writing of Alluxio configuration files.
 */
public class ConfigurationEditor {

  /**
   * Name of the shell script containing JVM configuration.
   */
  public static final String ENV_FILE = "alluxio-env.sh";

  /**
   * Name of the log4j properties file.
   */
  public static final String LOG_FILE = "log4j.properties";

  /**
   * Name of the site properties file.
   */
  public static final String SITE_PROPERTIES = Constants.SITE_PROPERTIES;

  @Nullable
  private Path mSiteProps;
  private Path mEnvSh;
  private final Path mLogProps;

  private final String[] mConfDirs;

  /**
   * Creates a new editor. It will search the paths supplied (delimited by ",") for the Alluxio
   * configuration files.
   *
   * @param siteConfDirs a comma-delimited list of paths to search for Alluxio configuration files
   */
  public ConfigurationEditor(String siteConfDirs) {
    mConfDirs = siteConfDirs.split(",");
    String res = ConfigurationUtils.searchPropertiesFile(SITE_PROPERTIES, mConfDirs);
    mSiteProps = res == null ? null : Paths.get(res);
    mEnvSh = searchConfFile(ENV_FILE, mConfDirs);
    Path log = searchConfFile(LOG_FILE, mConfDirs);
    mLogProps = log;
  }

  /**
   * @return the path that will be searched for the alluxio-site.properties file
   */
  public Path getSiteProperties() {
    return mSiteProps;
  }

  /**
   * @return the path that will be searched for alluxio-env.sh
   */
  @Nullable
  public Path getEnvSh() {
    return mEnvSh;
  }

  /**
   * @return the path that will be searched for log4j.properties
   */
  public Path getLogProps() {
    return mLogProps;
  }

  /**
   * Searches for the conf file in the list of supplied paths.
   *
   * @param fileName conf file name
   * @param siteConfDirs a list of paths to search for alluxio-env.sh
   * @return the path to the alluxio-env.sh, or {@code null} if not found
   */
  @Nullable
  static Path searchConfFile(String fileName, String[] siteConfDirs) {
    for (String path : siteConfDirs) {
      Path p = Paths.get(path).resolve(fileName);
      if (Files.exists(p)) {
        return p;
      }
    }
    return null;
  }

  /**
   * Reads the Alluxio configuration given the reader's configuration.
   *
   * @return the corresponding {@link AlluxioConfigurationSet}
   * @throws IOException if reading any of the configuration files fails
   */
  public AlluxioConfigurationSet readConf() throws IOException {
    byte[] sitePropData = mSiteProps == null ? new byte[0] : Files.readAllBytes(mSiteProps);
    byte[] envShData = mEnvSh == null ? new byte[0] : Files.readAllBytes(mEnvSh);
    byte[] logData = mLogProps == null ? new byte[0] : Files.readAllBytes(mLogProps);

    String sp = new String(sitePropData, Charset.defaultCharset());
    String env = new String(envShData, Charset.defaultCharset());
    String log = new String(logData, Charset.defaultCharset());
    return AlluxioConfigurationSet.newBuilder()
        .setAlluxioEnv(env)
        .setSiteProperties(sp)
        .setLog4JProperties(log)
        .build();
  }

  /**
   * Writes out the given configuration to disk.
   *
   * Upon failure, this method will throw an {@link IOException}, but will not undo or roll-back
   * any changes made to the files over the course of this function.
   *
   * @param conf the configuration to write
   * @throws IOException if any failures occur while writing
   */
  public void writeConf(AlluxioConfigurationSet conf) throws IOException {
    if (conf.hasSiteProperties()) {
      if (mSiteProps == null && mConfDirs.length > 1) {
        mSiteProps = Paths.get(PathUtils.concatPath(mConfDirs[0], SITE_PROPERTIES));
      }
      if (mSiteProps != null) {
        Files.write(mSiteProps, conf.getSiteProperties().getBytes());
      }
    }
    if (conf.hasAlluxioEnv()) {
      if (mEnvSh == null && mConfDirs.length > 1) {
        mEnvSh = Paths.get(PathUtils.concatPath(mConfDirs[0], ENV_FILE));
      }
      if (mEnvSh != null) {
        Files.write(mEnvSh, conf.getAlluxioEnv().getBytes());
      }
    }
    if (conf.hasLog4JProperties()) {
      Files.write(mLogProps, conf.getLog4JProperties().getBytes());
    }
  }
}
