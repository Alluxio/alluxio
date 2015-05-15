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

package tachyon.underfs;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.conf.TachyonConf;
import tachyon.Constants;
import tachyon.Pair;
import tachyon.TachyonURI;

/**
 * Tachyon stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system
 */
public abstract class UnderFileSystem {
  protected final TachyonConf mTachyonConf;

  /**
   * this variable indicates whether the underFS actually provides storage. Most UnderFS should
   * provide storage, but a dummyFS for example does not.
   */
  private boolean mProvidesStorage = true;

  public enum SpaceType {
    SPACE_TOTAL(0), SPACE_FREE(1), SPACE_USED(2);

    private final int mValue;

    private SpaceType(int value) {
      mValue = value;
    }

    /**
     * Get the integer value of this enum value.
     */
    public int getValue() {
      return mValue;
    }
  }

  /**
   * Get the UnderFileSystem instance according to its schema.
   *
   * @param path file path storing over the ufs.
   * @param tachyonConf the {@link tachyon.conf.TachyonConf} instance.
   * @throws IllegalArgumentException for unknown scheme
   * @return instance of the under layer file system
   */
  public static UnderFileSystem get(String path, TachyonConf tachyonConf) {
    return get(path, null, tachyonConf);
  }

  /**
   * Get the UnderFileSystem instance according to its scheme and configuration.
   *
   * @param path file path storing over the ufs
   * @param ufsConf the configuration object for ufs only
   * @param tachyonConf the {@link tachyon.conf.TachyonConf} instance.
   * @throws IllegalArgumentException for unknown scheme
   * @return instance of the under layer file system
   */
  public static UnderFileSystem get(String path, Object ufsConf, TachyonConf tachyonConf) {
    Preconditions.checkArgument(path != null, "path may not be null");

    // Use the registry to determine the factory to use to create the client
    return UnderFileSystemRegistry.create(path, tachyonConf, ufsConf);
  }

  /**
   * Determines if the given path is on a Hadoop under file system
   *
   * To decide if a path should use the hadoop implementation, we check
   * {@link String#startsWith(String)} to see if the configured schemas are found.
   */
  public static boolean isHadoopUnderFS(final String path, TachyonConf tachyonConf) {
    // TODO In Hadoop 2.x this can be replaced with the simpler call to
    // FileSystem.getFileSystemClass() without any need for having users explicitly declare the file
    // system schemes to treat as being HDFS
    // However as long as pre 2.x versions of Hadoop are supported this is not an option and we have
    // to continue to use this method
    for (final String prefix : tachyonConf.getList(Constants.UNDERFS_HADOOP_PREFIXS, ",", null)) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether the underFS provides storage
   */
  public boolean providesStorage() {
    return mProvidesStorage;
  }

  /**
   * Transform an input string like hdfs://host:port/dir, hdfs://host:port, file:///dir, /dir into a
   * pair of address and path. The returned pairs are ("hdfs://host:port", "/dir"),
   * ("hdfs://host:port", "/"), and ("/", "/dir"), respectively.
   *
   * @param path the input path string
   * @return null if path does not start with tachyon://, tachyon-ft://, hdfs://, s3://, s3n://,
   *         file://, /. Or a pair of strings denoting the under FS address and the relative path
   *         relative to that address. For local FS (with prefixes file:// or /), the under FS
   *         address is "/" and the path starts with "/".
   */
  public static Pair<String, String> parse(TachyonURI path, TachyonConf tachyonConf) {
    Preconditions.checkNotNull(path);

    if (path.hasScheme()) {
      String header = path.getScheme() + "://";
      String authority = (path.hasAuthority()) ? path.getAuthority() : "";
      if (header.equals(Constants.HEADER) || header.equals(Constants.HEADER_FT)
          || isHadoopUnderFS(header, tachyonConf) || header.equals(Constants.HEADER_S3N)) {
        if (path.getPath().isEmpty()) {
          return new Pair<String, String>(header + authority, TachyonURI.SEPARATOR);
        } else {
          return new Pair<String, String>(header + authority, path.getPath());
        }
      } else if (header.equals("file://")) {
        return new Pair<String, String>(TachyonURI.SEPARATOR, path.getPath());
      }
    } else if (path.isPathAbsolute()) {
      return new Pair<String, String>(TachyonURI.SEPARATOR, path.getPath());
    }

    return null;
  }

  protected UnderFileSystem(TachyonConf tachyonConf) {
    mTachyonConf = tachyonConf;
  }

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given master host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param conf Tachyon configuration
   * @param hostname The host that wants to connect to the under file system
   * @throws IOException
   */
  public abstract void connectFromMaster(TachyonConf conf, String hostname) throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given worker host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param conf Tachyon configuration
   * @param hostname The host that wants to connect to the under file system
   * @throws IOException
   */
  public abstract void connectFromWorker(TachyonConf conf, String hostname) throws IOException;

  /**
   * Closes this under file system.
   *
   * @throws IOException
   */
  public abstract void close() throws IOException;

  /**
   * Creates a file in the under file system with the indicated name.
   *
   * @param path The file name
   * @return A {@code OutputStream} object
   * @throws IOException
   */
  public abstract OutputStream create(String path) throws IOException;

  /**
   * Creates a file in the under file system with the indicated name and block size.
   *
   * @param path The file name
   * @param blockSizeByte The block size in bytes
   * @return A {@code OutputStream} object
   * @throws IOException
   */
  public abstract OutputStream create(String path, int blockSizeByte) throws IOException;

  /**
   * Creates a file in the under file system with the indicated name, replication number and block
   * size.
   *
   * @param path The file name
   * @param replication The number of replications for each block
   * @param blockSizeByte The block size in bytes
   * @return A {@code OutputStream} object
   * @throws IOException
   */
  public abstract OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException;

  /**
   * Deletes a file or folder from the under file system with the indicated name.
   *
   * @param path The file or folder name
   * @param recursive Whether we delete folder and its children
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public abstract boolean delete(String path, boolean recursive) throws IOException;

  /**
   * Checks if a file or folder exists in under file system.
   *
   * @param path The file name
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public abstract boolean exists(String path) throws IOException;

  /**
   * Gets the block size of a file in under file system, in bytes.
   *
   * @param path The file name
   * @return file size in bytes
   * @throws IOException
   */
  public abstract long getBlockSizeByte(String path) throws IOException;

  /**
   * Gets the configuration object for UnderFileSystem.
   *
   * @return configuration object used for concrete ufs instance
   */
  public abstract Object getConf();

  /**
   * Gets the list of locations of the indicated path.
   *
   * @param path The file name
   * @return The list of locations
   * @throws IOException
   */
  public abstract List<String> getFileLocations(String path) throws IOException;

  /**
   * Gets the list of locations of the indicated path given its offset.
   *
   * @param path The file name
   * @param offset Offset in bytes
   * @return The list of locations
   * @throws IOException
   */
  public abstract List<String> getFileLocations(String path, long offset) throws IOException;

  /**
   * Gets the file size in bytes.
   *
   * @param path The file name
   * @return the file size in bytes
   * @throws IOException
   */
  public abstract long getFileSize(String path) throws IOException;

  /**
   * Gets the UTC time of when the indicated path was modified recently in ms.
   *
   * @param path The file or folder name
   * @return modification time in milliseconds
   * @throws IOException
   */
  public abstract long getModificationTimeMs(String path) throws IOException;

  /**
   * Queries the under file system about the space of the indicated path (e.g., space left, space
   * used and etc).
   *
   * @param path The path to query
   * @param type The type of queries
   * @return The space in bytes.
   * @throws IOException
   */
  public abstract long getSpace(String path, SpaceType type) throws IOException;

  /**
   * Checks if the indicated path is a file or not.
   *
   * @param path The path name
   * @return true if this is a file, false otherwise
   * @throws IOException
   */
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
   * @param path the path to list.
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
   * @param path the folder to create
   * @param createParent If true, the method creates any necessary but nonexistent parent
   *        directories. Otherwise, the method does not create nonexistent parent directories.
   * @return <code>true</code> if and only if the directory was created; <code>false</code>
   *         otherwise
   * @throws IOException
   */
  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  /**
   * Opens an {@code InputStream} at the indicated path.
   *
   * @param path The file name
   * @return The {@code InputStream} object
   * @throws IOException
   */
  public abstract InputStream open(String path) throws IOException;

  /**
   * Renames a file or folder from {@code src} to {@code dst} in under file system
   *
   * @param src The source file or folder name
   * @param dst The destination file or folder name
   * @return true if succeed, false otherwise
   * @throws IOException
   */
  public abstract boolean rename(String src, String dst) throws IOException;

  /**
   * To set the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete underfs's implementation.
   *
   * @param conf The configuration object accepted by ufs.
   */
  public abstract void setConf(Object conf);

  /**
   * Change posix file permission
   *
   * @param path path of the file
   * @param posixPerm standard posix permission like "777", "775", etc.
   * @throws IOException
   */
  public abstract void setPermission(String path, String posixPerm) throws IOException;
}
