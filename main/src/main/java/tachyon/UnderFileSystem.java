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

  /**
   * Get the UnderFileSystem instance according to its schema.
   *
   * @param path
   *          file path storing over the ufs.
   * @return null for any unknown scheme.
   */
  public static UnderFileSystem get(String path) {
    return get(path, null);
  }

  /**
   * Get the UnderFileSystem instance according to its scheme and configuration.
   *
   * @param path
   *          file path storing over the ufs
   * @param conf
   *          the configuration object for ufs only
   * @return null for any unknown scheme.
   */
  public static UnderFileSystem get(String path, Object conf) {
    if (path.startsWith("hdfs://") || path.startsWith("s3://") || path.startsWith("s3n://")) {
      return UnderFileSystemHdfs.getClient(path, conf);
    } else if (path.startsWith(Constants.PATH_SEPARATOR) || path.startsWith("file://")) {
      return UnderFileSystemSingleLocal.getClient();
    }
    CommonUtils.illegalArgumentException("Unknown under file system scheme " + path);
    return null;
  }

  /**
   * Transform an input string like hdfs://host:port/dir, hdfs://host:port, file:///dir, /dir
   * into a pair of address and path. The returned pairs are ("hdfs://host:port", "/dir"),
   * ("hdfs://host:port", "/"), and ("/", "/dir"), respectively.
   *
   * @param path
   *          the input path string
   * @return null if path does not start with tachyon://, tachyon-ft://, hdfs://, s3://, s3n://,
   *         file://, /. Or a pair of strings denoting the under FS address and the relative path
   *         relative to that address. For local FS (with prefixes file:// or /), the under FS
   *         address is "/" and the path starts with "/".
   */
  public static Pair<String, String> parse(String path) {
    if (path == null) {
      return null;
    } else if (path.startsWith("tachyon://") || path.startsWith("tachyon-ft://")
        || path.startsWith("hdfs://") || path.startsWith("s3://") || path.startsWith("s3n://")) {
      String prefix = path.substring(0, path.indexOf("://") + 3);
      String body = path.substring(prefix.length());
      if (body.contains(Constants.PATH_SEPARATOR)) {
        int ind = body.indexOf(Constants.PATH_SEPARATOR);
        return new Pair<String, String>(prefix + body.substring(0, ind), body.substring(ind));
      } else {
        return new Pair<String, String>(path, Constants.PATH_SEPARATOR);
      }
    } else if (path.startsWith("file://") || path.startsWith(Constants.PATH_SEPARATOR)) {
      String prefix = "file://";
      String suffix = path.startsWith(prefix) ? path.substring(prefix.length()) : path;
      return new Pair<String, String>(Constants.PATH_SEPARATOR, suffix);
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

  /**
   * To get the configuration object for UnderFileSystem.
   *
   * @return configuration object used for concrete ufs instance
   */
  public abstract Object getConf();

  public abstract List<String> getFileLocations(String path) throws IOException;

  public abstract List<String> getFileLocations(String path, long offset) throws IOException;

  public abstract long getFileSize(String path) throws IOException;

  public abstract long getModificationTimeMs(String path) throws IOException;

  public abstract long getSpace(String path, SpaceType type) throws IOException;

  public abstract boolean isFile(String path) throws IOException;

  /**
   * Returns an array of strings naming the files and directories in the directory denoted by this
   * abstract pathname.
   *
   * <p>
   * If this abstract pathname does not denote a directory, then this method returns {@code null}.
   * Otherwise an array of strings is returned, one for each file or directory in the directory.
   * Names denoting the directory itself and the directory's parent directory are not included in
   * the result. Each string is a file name rather than a complete path.
   *
   * <p>
   * There is no guarantee that the name strings in the resulting array will appear in any specific
   * order; they are not, in particular, guaranteed to appear in alphabetical order.
   *
   * @param path
   *          the path to list.
   * @return An array of strings naming the files and directories in the directory denoted by this
   *         abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory, or if an I/O error
   *         occurs.
   * @throws IOException
   */
  public abstract String[] list(String path) throws IOException;

  /**
   * Creates the directory named by this abstract pathname. If the folder already exists, the method
   * returns false.
   *
   * @param path
   *          the folder to create
   * @param createParent
   *          If true, the method creates any necessary but nonexistent parent directories.
   *          Otherwise, the method does not create nonexistent parent directories.
   * @return <code>true</code> if and only if the directory was created; <code>false</code>
   *         otherwise
   * @throws IOException
   */
  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  public abstract InputStream open(String path) throws IOException;

  public abstract boolean rename(String src, String dst) throws IOException;

  /**
   * To set the configuration object for UnderFileSystem.
   * The conf object is understood by the concrete underfs's implementation.
   *
   * @param conf
   *          The configuration object accepted by ufs.
   */
  public abstract void setConf(Object conf);

  /**
   * Change posix file permission
   *
   * @param path
   *          path of the file
   * @param posixPerm
   *          standard posix permission like "777", "775", etc.
   * @throws IOException
   */
  public abstract void setPermission(String path, String posixPerm) throws IOException;
}
