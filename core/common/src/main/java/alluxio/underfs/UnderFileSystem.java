/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system
 */
@ThreadSafe
public abstract class UnderFileSystem {
  protected final Configuration mConfiguration;

  /** The UFS {@link AlluxioURI} used to create this {@link UnderFileSystem}. */
  protected final AlluxioURI mUri;

  /** A map of property names to values. */
  protected HashMap<String, String> mProperties = new HashMap<>();

  /**
   * This variable indicates whether the underFS actually provides storage. Most UnderFS should
   * provide storage, but a dummyFS for example does not.
   */
  private boolean mProvidesStorage = true;

  /**
   * The different types of space indicate the total space, the free space and the space used in the
   * under file system.
   */
  public enum SpaceType {

    /**
     * Indicates the storage capacity of the under file system.
     */
    SPACE_TOTAL(0),

    /**
     * Indicates the amount of free space available in the under file system.
     */
    SPACE_FREE(1),

    /**
     * Indicates the amount of space used in the under file system.
     */
    SPACE_USED(2);

    private final int mValue;

    SpaceType(int value) {
      mValue = value;
    }

    /**
     * @return the integer value of this enum value
     */
    public int getValue() {
      return mValue;
    }
  }

  /**
   * Gets the UnderFileSystem instance according to its schema.
   *
   * @param path file path storing over the ufs
   * @param configuration the {@link Configuration} instance
   * @return instance of the under layer file system
   */
  public static UnderFileSystem get(String path, Configuration configuration) {
    return get(path, null, configuration);
  }

  /**
   * Gets the UnderFileSystem instance according to its scheme and configuration.
   *
   * @param path file path storing over the ufs
   * @param ufsConf the configuration object for ufs only
   * @param configuration the {@link Configuration} instance
   * @return instance of the under layer file system
   */
  public static UnderFileSystem get(String path, Object ufsConf, Configuration configuration) {
    Preconditions.checkArgument(path != null, "path may not be null");
    Preconditions.checkNotNull(configuration);

    // Use the registry to determine the factory to use to create the client
    return UnderFileSystemRegistry.create(path, configuration, ufsConf);
  }

  /**
   * Type of under filesystem, to be used by {@link #getUnderFSType()} to determine which concrete
   * under filesystem implementation is being used. New types of under filesystem should be added
   * below and returned by the implementation of {@link #getUnderFSType()}.
   */
  public enum UnderFSType {
    LOCAL,
    HDFS,
    S3,
    GLUSTERFS,
    SWIFT,
    OSS,
    GCS,
  }

  /**
   * @return type of concrete under filesystem implementation
   */
  public abstract UnderFSType getUnderFSType();

  /**
   * Determines if the given path is on a Hadoop under file system
   *
   * To decide if a path should use the hadoop implementation, we check
   * {@link String#startsWith(String)} to see if the configured schemas are found.
   *
   * @param path the path in under filesystem
   * @param configuration the configuration for Alluxio
   * @return true if the given path is on a Hadoop under file system, false otherwise
   */
  public static boolean isHadoopUnderFS(final String path, Configuration configuration) {
    // TODO(hy): In Hadoop 2.x this can be replaced with the simpler call to
    // FileSystem.getFileSystemClass() without any need for having users explicitly declare the file
    // system schemes to treat as being HDFS. However as long as pre 2.x versions of Hadoop are
    // supported this is not an option and we have to continue to use this method.
    for (final String prefix : configuration.getList(Constants.UNDERFS_HDFS_PREFIXS, ",")) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether the underFS provides storage.
   *
   * @return true if the under filesystem provides storage, false otherwise
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
   * @param configuration the configuration for Alluxio
   * @return null if path does not start with alluxio://, alluxio-ft://, hdfs://, s3://, s3n://,
   *         file://, /. Or a pair of strings denoting the under FS address and the relative path
   *         relative to that address. For local FS (with prefixes file:// or /), the under FS
   *         address is "/" and the path starts with "/".
   */
  public static Pair<String, String> parse(AlluxioURI path, Configuration configuration) {
    Preconditions.checkNotNull(path);

    if (path.hasScheme()) {
      String header = path.getScheme() + "://";
      String authority = (path.hasAuthority()) ? path.getAuthority() : "";
      if (header.equals(Constants.HEADER) || header.equals(Constants.HEADER_FT)
          || isHadoopUnderFS(header, configuration) || header.equals(Constants.HEADER_S3)
          || header.equals(Constants.HEADER_S3N) || header.equals(Constants.HEADER_OSS)
          || header.equals(Constants.HEADER_GCS)) {
        if (path.getPath().isEmpty()) {
          return new Pair<String, String>(header + authority, AlluxioURI.SEPARATOR);
        } else {
          return new Pair<String, String>(header + authority, path.getPath());
        }
      } else if (header.equals("file://")) {
        return new Pair<String, String>(AlluxioURI.SEPARATOR, path.getPath());
      }
    } else if (path.isPathAbsolute()) {
      return new Pair<String, String>(AlluxioURI.SEPARATOR, path.getPath());
    }

    return null;
  }

  /**
   * Constructs an {@link UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} used to create this UFS
   * @param configuration the {@link Configuration} for this UFS
   */
  protected UnderFileSystem(AlluxioURI uri, Configuration configuration) {
    Preconditions.checkNotNull(uri);
    Preconditions.checkNotNull(configuration);
    mUri = uri;
    mConfiguration = configuration;
  }

  /**
   * Configures and updates the properties. For instance, this method can add new properties or
   * modify existing properties specified through {@link #setProperties(Map)}.
   *
   * The default implementation is a no-op. This should be overridden if a subclass needs
   * additional functionality.
   * @throws IOException if an error occurs during configuration
   */
  public void configureProperties() throws IOException {
    // Default implementation does not update any properties.
  }

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given master host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param conf Alluxio configuration
   * @param hostname The host that wants to connect to the under file system
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract void connectFromMaster(Configuration conf, String hostname) throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given worker host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param conf Alluxio configuration
   * @param hostname The host that wants to connect to the under file system
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract void connectFromWorker(Configuration conf, String hostname) throws IOException;

  /**
   * Closes this under file system.
   *
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract void close() throws IOException;

  /**
   * Creates a file in the under file system with the indicated name.
   *
   * @param path The file name
   * @return A {@code OutputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract OutputStream create(String path) throws IOException;

  /**
   * Creates a file in the under file system with the indicated name and block size.
   *
   * @param path The file name
   * @param blockSizeByte The block size in bytes
   * @return A {@code OutputStream} object
   * @throws IOException if a non-Alluxio error occurs
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
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract OutputStream create(String path, short replication, int blockSizeByte)
      throws IOException;

  /**
   * Deletes a file or folder from the under file system with the indicated name.
   *
   * @param path The file or folder name
   * @param recursive Whether we delete folder and its children
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract boolean delete(String path, boolean recursive) throws IOException;

  /**
   * Checks if a file or folder exists in under file system.
   *
   * @param path The file name
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract boolean exists(String path) throws IOException;

  /**
   * Gets the block size of a file in under file system, in bytes.
   *
   * @param path The file name
   * @return the block size in bytes
   * @throws IOException if a non-Alluxio error occurs
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
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract List<String> getFileLocations(String path) throws IOException;

  /**
   * Gets the list of locations of the indicated path given its offset.
   *
   * @param path The file name
   * @param offset Offset in bytes
   * @return The list of locations
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract List<String> getFileLocations(String path, long offset) throws IOException;

  /**
   * Gets the file size in bytes.
   *
   * @param path The file name
   * @return the file size in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract long getFileSize(String path) throws IOException;

  /**
   * Gets the UTC time of when the indicated path was modified recently in ms.
   *
   * @param path The file or folder name
   * @return modification time in milliseconds
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract long getModificationTimeMs(String path) throws IOException;

  /**
   * @return the property map for this {@link UnderFileSystem}
   */
  public Map<String, String> getProperties() {
    return Collections.unmodifiableMap(mProperties);
  }

  /**
   * Queries the under file system about the space of the indicated path (e.g., space left, space
   * used and etc).
   *
   * @param path The path to query
   * @param type The type of queries
   * @return The space in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract long getSpace(String path, SpaceType type) throws IOException;

  /**
   * Checks if the indicated path is a file or not.
   *
   * @param path The path name
   * @return true if this is a file, false otherwise
   * @throws IOException if a non-Alluxio error occurs
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
   * @param path the abstract pathname to list
   * @return An array of strings naming the files and directories in the directory denoted by this
   *         abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract String[] list(String path) throws IOException;

  /**
   * Returns an array of strings naming the files and directories in the directory denoted by this
   * abstract pathname, and all of its subdirectories.
   *
   * <p>
   * If this abstract pathname does not denote a directory, then this method returns {@code null}.
   * Otherwise an array of strings is returned, one for each file or directory in the directory and
   * its subdirectories. Names denoting the directory itself and the directory's parent directory
   * are not included in the result. Each string is a path relative to the given directory.
   *
   * <p>
   * There is no guarantee that the name strings in the resulting array will appear in any specific
   * order; they are not, in particular, guaranteed to appear in alphabetical order.
   *
   * @param path the abstract pathname to list
   * @return An array of strings naming the files and directories in the directory denoted by this
   *         abstract pathname and its subdirectories. The array will be empty if the directory is
   *         empty. Returns {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException if a non-Alluxio error occurs
   */
  public String[] listRecursive(String path) throws IOException {
    // Clean the path by creating a URI and turning it back to a string
    AlluxioURI uri = new AlluxioURI(path);
    path = uri.toString();
    List<String> returnPaths = new ArrayList<String>();
    Queue<String> pathsToProcess = new ArrayDeque<String>();
    // We call list initially, so we can return null if the path doesn't denote a directory
    String[] subpaths = list(path);
    if (subpaths == null) {
      return null;
    } else {
      for (String subp : subpaths) {
        pathsToProcess.add(PathUtils.concatPath(path, subp));
      }
    }
    while (!pathsToProcess.isEmpty()) {
      String p = pathsToProcess.remove();
      returnPaths.add(p.substring(path.length() + 1));
      // Add all of its subpaths
      subpaths = list(p);
      if (subpaths != null) {
        for (String subp : subpaths) {
          pathsToProcess.add(PathUtils.concatPath(p, subp));
        }
      }
    }
    return returnPaths.toArray(new String[returnPaths.size()]);
  }

  /**
   * Creates the directory named by this abstract pathname. If the folder already exists, the method
   * returns false.
   *
   * @param path the folder to create
   * @param createParent If true, the method creates any necessary but nonexistent parent
   *        directories. Otherwise, the method does not create nonexistent parent directories.
   * @return {@code true} if and only if the directory was created; {@code false}
   *         otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract boolean mkdirs(String path, boolean createParent) throws IOException;

  /**
   * Opens an {@link InputStream} at the indicated path.
   *
   * @param path The file name
   * @return The {@code InputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract InputStream open(String path) throws IOException;

  /**
   * Renames a file or folder from {@code src} to {@code dst} in under file system.
   *
   * @param src The source file or folder name
   * @param dst The destination file or folder name
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract boolean rename(String src, String dst) throws IOException;

  /**
   * Returns an {@link AlluxioURI} representation for the {@link UnderFileSystem} given a base
   * UFS URI, and the Alluxio path from the base.
   *
   * The default implementation simply concatenates the path to the base URI. This should be
   * overridden if a subclass needs alternate functionality.
   *
   * @param ufsBaseUri the base {@link AlluxioURI} in the UFS
   * @param alluxioPath the path in Alluxio from the given base
   * @return the UFS {@link AlluxioURI} representing the Alluxio path
   */
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return new AlluxioURI(ufsBaseUri.getScheme(), ufsBaseUri.getAuthority(),
        PathUtils.concatPath(ufsBaseUri.getPath(), alluxioPath), ufsBaseUri.getQueryMap());
  }

  /**
   * Sets the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete underfs's implementation.
   *
   * @param conf the configuration object accepted by ufs
   */
  public abstract void setConf(Object conf);

  /**
   * Sets the properties for this {@link UnderFileSystem}.
   *
   * @param properties a {@link Map} of property names to values
   */
  public void setProperties(Map<String, String> properties) {
    mProperties.clear();
    mProperties.putAll(properties);
  }

  /**
   * Changes posix file permission.
   *
   * @param path path of the file
   * @param posixPerm standard posix permission like "777", "775", etc
   * @throws IOException if a non-Alluxio error occurs
   */
  public abstract void setPermission(String path, String posixPerm) throws IOException;
}
