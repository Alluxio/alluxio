/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.log4j.Logger;

import tachyon.Constants;

public final class Utils {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private static final boolean DEBUG = Constants.DEBUG;

  public static Path getHDFSPath(String path) {
    path = getTachyonFileName(path);

    String mid = Constants.PATH_SEPARATOR;
    if (path.startsWith(Constants.PATH_SEPARATOR)) {
      mid = "";
    }

    return new Path(TFS.UNDERFS_ADDRESS + mid + path);
  }

  public static String getPathWithoutScheme(Path path) {
    Path ori = path;
    String ret = "";
    while (path != null) {
      if (ret.equals("")) {
        ret = path.getName();
      } else {
        ret = path.getName() + Constants.PATH_SEPARATOR + ret;
      }
      path = path.getParent();
    }
    if (DEBUG) {
      LOG.info("Utils getPathWithoutScheme(" + ori + ") result: " + ret);
    }
    if (ret.isEmpty()) {
      return Constants.PATH_SEPARATOR;
    }
    return ret;
  }

  public static String getTachyonFileName(String path) {
    if (path.isEmpty()) {
      return Constants.PATH_SEPARATOR;
    }

    while (path.contains(":")) {
      int index = path.indexOf(":");
      path = path.substring(index + 1);
    }

    while (!path.startsWith(Constants.PATH_SEPARATOR)) {
      path = path.substring(1);
    }

    return path;
  }

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
}
