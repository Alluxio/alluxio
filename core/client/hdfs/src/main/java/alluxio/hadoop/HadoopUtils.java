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

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.util.ConfigurationUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class for using Alluxio with Hadoop.
 */
@ThreadSafe
public final class HadoopUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

  /**
   * Given a {@link Path} path, it returns the path component of its URI, which has the form
   * scheme://authority/path.
   *
   * @param path an HDFS {@link Path}
   * @return the path component of the {@link Path} URI
   */
  public static String getPathWithoutScheme(Path path) {
    return path.toUri().getPath();
  }

  /**
   * Given a {@code String} path, returns an equivalent Alluxio path.
   *
   * @param path the path to parse
   * @return a valid Alluxio path
   */
  public static String getAlluxioFileName(String path) {
    if (path.isEmpty()) {
      return AlluxioURI.SEPARATOR;
    }

    while (path.contains(":")) {
      int index = path.indexOf(":");
      path = path.substring(index + 1);
    }

    while (!path.startsWith(AlluxioURI.SEPARATOR)) {
      path = path.substring(1);
    }

    return path;
  }

  /**
   * Returns a string representation of a Hadoop {@link FileSplit}.
   *
   * @param fs Hadoop {@link FileSplit}
   * @return its string representation
   */
  public static String toStringHadoopFileSplit(FileSplit fs) {
    StringBuilder sb = new StringBuilder();
    sb.append("HadoopFileSplit: Path: ").append(fs.getPath());
    sb.append(" , Start: ").append(fs.getStart());
    sb.append(" , Length: ").append(fs.getLength());
    sb.append(" , Hosts: ");
    String[] locs;
    try {
      locs = fs.getLocations();
    } catch (IOException e) {
      LOG.error(e.getMessage());
      locs = new String[] {};
    }
    for (String loc : locs) {
      sb.append(loc).append("; ");
    }

    return sb.toString();
  }

  /**
   * Returns a string representation of a Hadoop {@link FileStatus}.
   *
   * @param fs Hadoop {@link FileStatus}
   * @return its string representation
   */
  public static String toStringHadoopFileStatus(FileStatus fs) {
    StringBuilder sb = new StringBuilder();
    sb.append("HadoopFileStatus: Path: ").append(fs.getPath());
    sb.append(" , Length: ").append(fs.getLen());
    // Use isDir instead of isDirectory for compatibility with hadoop 1.
    sb.append(" , IsDir: ").append(fs.isDir());
    sb.append(" , BlockReplication: ").append(fs.getReplication());
    sb.append(" , BlockSize: ").append(fs.getBlockSize());
    sb.append(" , ModificationTime: ").append(fs.getModificationTime());
    sb.append(" , AccessTime: ").append(fs.getAccessTime());
    sb.append(" , Permission: ").append(fs.getPermission());
    sb.append(" , Owner: ").append(fs.getOwner());
    sb.append(" , Group: ").append(fs.getGroup());
    return sb.toString();
  }

  /**
   * Adds Swift keys to the given Hadoop Configuration object if the user has specified them using
   * System properties, and they're not already set.
   *
   * This function is duplicated from {@code alluxio.underfs.hdfs.HdfsUnderFileSystemUtils}, to
   * prevent the module alluxio-core-client-hdfs from depending on the module alluxio-underfs.
   *
   * @param configuration Hadoop configuration
   */

  public static void addSwiftCredentials(Configuration configuration) {
    PropertyKey[] propertyNames = { PropertyKey.SWIFT_TENANT_KEY, PropertyKey.SWIFT_USER_KEY,
        PropertyKey.SWIFT_AUTH_URL_KEY, PropertyKey.SWIFT_AUTH_METHOD_KEY,
        PropertyKey.SWIFT_PASSWORD_KEY, PropertyKey.SWIFT_SIMULATION,
        PropertyKey.SWIFT_REGION_KEY };
    setConfigurationFromSystemProperties(configuration, propertyNames);
  }

  /**
   * Set the System properties into Hadoop configuration.
   *
   * This method won't override existing properties even if they are set as System properties.
   *
   * @param configuration Hadoop configuration
   * @param propertyNames the properties to be set
   */
  private static void setConfigurationFromSystemProperties(Configuration configuration,
      PropertyKey[] propertyNames) {
    for (PropertyKey propertyName : propertyNames) {
      setConfigurationFromSystemProperty(configuration, propertyName.toString());
    }
  }

  /**
   * Set the System property into Hadoop configuration.
   *
   * This method won't override existing property even if it is set as System property.
   *
   * @param configuration Hadoop configuration
   * @param propertyName the property to be set
   */
  private static void setConfigurationFromSystemProperty(Configuration configuration,
      String propertyName) {
    String propertyValue = System.getProperty(propertyName);
    if (propertyValue != null && configuration.get(propertyName) == null) {
      configuration.set(propertyName, propertyValue);
    }
  }

  /**
   * @param alluxioURI Alluxio uri
   * @return corresponding Hadoop Path instance
   */
  public static Path toPath(AlluxioURI alluxioURI) {
    return new Path(alluxioURI.toString());
  }

  /**
   * @param conf Hadoop conf
   * @return Alluxio configuration merged from Hadoop conf
   */
  public static AlluxioConfiguration toAlluxioConf(Configuration conf) {
    // Take hadoop configuration to merge to Alluxio configuration
    Map<String, Object> hadoopConfProperties =
        HadoopConfigurationUtils.getConfigurationFromHadoop(conf);
    AlluxioProperties alluxioProps = ConfigurationUtils.defaults();
    // Merge relevant Hadoop configuration into Alluxio's configuration.
    alluxioProps.merge(hadoopConfProperties, Source.RUNTIME);
    // Creating a new instanced configuration from an AlluxioProperties object isn't expensive.
    return new InstancedConfiguration(alluxioProps);
  }

  private HadoopUtils() {} // prevent instantiation
}
