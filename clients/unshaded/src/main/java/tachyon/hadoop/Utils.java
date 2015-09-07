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

package tachyon.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;

public final class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Returns an HDFS path for the given Tachyon path and under filesystem address.
   *
   * @param path Tachyon path
   * @param ufsAddress under filesystem address
   * @return an HDFS path
   */
  public static Path getHDFSPath(TachyonURI path, String ufsAddress) {
    if (path.isPathAbsolute()) {
      return new Path(ufsAddress + path.getPath());
    } else {
      return new Path(ufsAddress + TachyonURI.SEPARATOR + path.getPath());
    }
  }

  /**
   * Given a <code>Path</code> path, it returns the path component of its URI, which has the form
   * scheme://authority/path.
   *
   * @param path an HDFS <code>Path</code>
   * @return the path component of the <code>Path</code> URI
   */
  public static String getPathWithoutScheme(Path path) {
    return path.toUri().getPath();
  }

  /**
   * Given a <code>String</code> path, it returns an equivalent Tachyon path.
   *
   * @param path the path to parse
   * @return a valid Tachyon path
   */
  public static String getTachyonFileName(String path) {
    if (path.isEmpty()) {
      return TachyonURI.SEPARATOR;
    }

    while (path.contains(":")) {
      int index = path.indexOf(":");
      path = path.substring(index + 1);
    }

    while (!path.startsWith(TachyonURI.SEPARATOR)) {
      path = path.substring(1);
    }

    return path;
  }

  /**
   * Returns a string representation of a Hadoop <code>FileSplit</code>.
   *
   * @param fs Hadoop <code>FileSplit</code>
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
   * Returns a string representation of a Hadoop <code>FileStatus</code>.
   *
   * @param fs Hadoop <code>FileStatus</code>
   * @return its string representation
   */
  public static String toStringHadoopFileStatus(FileStatus fs) {
    StringBuilder sb = new StringBuilder();
    sb.append("HadoopFileStatus: Path: ").append(fs.getPath());
    sb.append(" , Length: ").append(fs.getLen());
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
   * Returns a string representation of a <code>InputSplit</code>.
   *
   * @param is Hadoop <code>InputSplit</code>
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
   * This function is duplicated from tachyon.underfs.hdfs.HdfsUnderFileSystemUtils, to prevent the
   * module tachyon-client from depending on the module tachyon-underfs.
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
}
