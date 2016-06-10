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
import alluxio.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilty class for using Alluxio with Hadoop.
 */
@ThreadSafe
public final class HadoopUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
    sb.append(" , IsDir: ").append(fs.isDirectory());
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
   * Returns a string representation of a {@link InputSplit}.
   *
   * @param is Hadoop {@link InputSplit}
   * @return its string representation
   */
  public static String toStringHadoopInputSplit(InputSplit is) {
    StringBuilder sb = new StringBuilder("HadoopInputSplit: ");
    try {
      sb.append(" Length: ").append(is.getLength());
      sb.append(" , Locations: ");
      for (String loc : is.getLocations()) {
        sb.append(loc).append(" ; ");
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return sb.toString();
  }

  /**
   * Adds S3 keys to the given Hadoop Configuration object if the user has specified them using
   * System properties, and they're not already set.
   *
   * This function is duplicated from {@code alluxio.underfs.hdfs.HdfsUnderFileSystemUtils}, to
   * prevent the module alluxio-core-client from depending on the module alluxio-underfs.
   *
   * TODO(hy): Remove duplication in the future.
   *
   * @param conf Hadoop configuration
   */
  public static void addS3Credentials(Configuration conf) {
    String accessKeyConf = Constants.S3_ACCESS_KEY;
    if (System.getProperty(accessKeyConf) != null && conf.get(accessKeyConf) == null) {
      conf.set(accessKeyConf, System.getProperty(accessKeyConf));
    }
    String secretKeyConf = Constants.S3_SECRET_KEY;
    if (System.getProperty(secretKeyConf) != null && conf.get(secretKeyConf) == null) {
      conf.set(secretKeyConf, System.getProperty(secretKeyConf));
    }
  }

  /**
   * Adds Swift keys to the given Hadoop Configuration object if the user has specified them using
   * System properties, and they're not already set.
   *
   * This function is duplicated from {@code alluxio.underfs.hdfs.HdfsUnderFileSystemUtils}, to
   * prevent the module alluxio-core-client from depending on the module alluxio-underfs.
   *
   * TODO(hy): Remove duplication in the future.
   *
   * @param configuration Hadoop configuration
   */

  public static void addSwiftCredentials(Configuration configuration) {
    String tenantApiKeyConf = Constants.SWIFT_API_KEY;
    if (System.getProperty(tenantApiKeyConf) != null
        && configuration.get(tenantApiKeyConf) == null) {
      configuration.set(tenantApiKeyConf, System.getProperty(tenantApiKeyConf));
    }
    String tenantKeyConf = Constants.SWIFT_TENANT_KEY;
    if (System.getProperty(tenantKeyConf) != null
        && configuration.get(tenantKeyConf) == null) {
      configuration.set(tenantKeyConf, System.getProperty(tenantKeyConf));
    }
    String tenantUserConf = Constants.SWIFT_USER_KEY;
    if (System.getProperty(tenantUserConf) != null
        && configuration.get(tenantUserConf) == null) {
      configuration.set(tenantUserConf, System.getProperty(tenantUserConf));
    }
    String tenantAuthURLKeyConf = Constants.SWIFT_AUTH_URL_KEY;
    if (System.getProperty(tenantAuthURLKeyConf) != null
        && configuration.get(tenantAuthURLKeyConf) == null) {
      configuration.set(tenantAuthURLKeyConf, System.getProperty(tenantAuthURLKeyConf));
    }
    String authMethodKeyConf = Constants.SWIFT_AUTH_METHOD_KEY;
    if (System.getProperty(authMethodKeyConf) != null
        && configuration.get(authMethodKeyConf) == null) {
      configuration.set(authMethodKeyConf, System.getProperty(authMethodKeyConf));
    }
    String passwordKeyConf = Constants.SWIFT_PASSWORD_KEY;
    if (System.getProperty(passwordKeyConf) != null
        && configuration.get(passwordKeyConf) == null) {
      configuration.set(passwordKeyConf, System.getProperty(passwordKeyConf));
    }
  }

  private HadoopUtils() {} // prevent instantiation
}
