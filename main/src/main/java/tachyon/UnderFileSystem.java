/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import tachyon.util.CommonUtils;

/**
 * Tachyon stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system
 */
public abstract class UnderFileSystem {
  public enum SpaceType {
    SPACE_TOTAL(0), SPACE_FREE(1), SPACE_USED(2);

    private final int value;

    private SpaceType(int value) {
      this.value = value;
    }

    /**
     * Get the integer value of this enum value.
     */
    public int getValue() {
      return value;
    }
  }

  public static UnderFileSystem get(String path) {
    if (path.startsWith("hdfs://")
        || path.startsWith("s3://") || path.startsWith("s3n://")) {
      return UnderFileSystemHdfs.getClient(path);
    } else if (path.startsWith(Constants.PATH_SEPARATOR)
        || path.startsWith("file://") || path.equals("")) {
      return UnderFileSystemSingleLocal.getClient();
    }
    CommonUtils.illegalArgumentException("Unknown under file system scheme " + path);
    return null;
  }

  /**
   * parse() transforms an input string like hdfs://host:port/dir, hdfs://host:port, /dir
   * into a pair of address and path. The returned pairs are ("hdfs://host:port", "/dir"),
   * ("hdfs://host:port", "/"), and ("", "/dir"), respectively.
   *
   * @param s
   * @return null if s does not start with tachon://, tachyon-ft://, hdfs://, s3://, s3n://,
   * file://, /. Or a pair of strings denoting the under FS address and the relative path
   * relative to that address. For local FS (with prefixes file:// or /), the under FS address
   * is "" and the path starts with "/".
   */
  public static String[] parse(String s) {
    if (s == null) {
      return null;
    } else if (s.startsWith("tachyon://") || s.startsWith("tachyon-ft://")
        || s.startsWith("hdfs://")
        || s.startsWith("s3://") || s.startsWith("s3n://")) {
      String prefix = s.substring(0, s.indexOf("://") + 3);
      String body = s.substring(prefix.length());
      String[] pair = new String[2];
      if (body.contains(Constants.PATH_SEPARATOR)) {
        int i = body.indexOf(Constants.PATH_SEPARATOR);
        pair[0] = prefix + body.substring(0, i);
        pair[1] = body.substring(i);
      } else {
        pair[0] = s;
        pair[1] = Constants.PATH_SEPARATOR;
      }
      return pair;
    } else if (s.startsWith("file://") || s.startsWith(Constants.PATH_SEPARATOR)) {
      String prefix = "file://";
      String suffix = s.startsWith(prefix) ? s.substring(prefix.length()) : s;
      return new String[]{"", suffix};
    }

    return null;
  }

  public abstract void close() throws IOException;

  public abstract OutputStream create(String path) throws IOException;

  public abstract OutputStream create(String path, int blockSizeByte) throws IOException;

  public abstract OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException;

  public abstract boolean delete(String path, boolean recursive) throws IOException;

  public abstract boolean exists(String path) throws IOException;

  public abstract long getBlockSizeByte(String path) throws IOException;

  public abstract List<String> getFileLocations(String path) throws IOException;

  public abstract List<String> getFileLocations(String path, long offset) throws IOException;

  public abstract long getFileSize(String path) throws IOException;

  public abstract long getModificationTimeMs(String path) throws IOException;

  public abstract long getSpace(String path, SpaceType type) throws IOException;

  public abstract boolean isFile(String path) throws IOException;

  /**
   * List all the files in the folder.
   *
   * @param path the path to list.
   * @return all the file names under the path.
   * @throws IOException
   */
  public abstract String[] list(String path) throws IOException;

  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  public abstract InputStream open(String path) throws IOException;

  public abstract boolean rename(String src, String dst) throws IOException;

  public abstract void toFullPermission(String path) throws IOException;
}
