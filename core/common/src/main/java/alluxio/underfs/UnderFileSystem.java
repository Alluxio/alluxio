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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system
 */
@ThreadSafe
public interface UnderFileSystem {

  /**
   * The factory for the {@link UnderFileSystem}.
   */
  class Factory {
    private static final Cache UFS_CACHE = new Cache();

    private Factory() {} // prevent instantiation

    /**
     * A class used to cache UnderFileSystems.
     */
    @ThreadSafe
    private static final class Cache {
      /**
       * Maps from {@link Key} to {@link UnderFileSystem} instances.
       */
      private final ConcurrentHashMap<Key, UnderFileSystem> mUnderFileSystemMap =
          new ConcurrentHashMap<>();

      private Cache() {}

      /**
       * Gets a UFS instance from the cache if exists. Otherwise, creates a new instance and adds
       * that to the cache.
       *
       * @param path the ufs path
       * @param ufsConf the ufs configuration
       * @return the UFS instance
       */
      UnderFileSystem get(String path, Object ufsConf) {
        Key key = new Key(new AlluxioURI(path));
        UnderFileSystem cachedFs = mUnderFileSystemMap.get(key);
        if (cachedFs != null) {
          return cachedFs;
        }
        UnderFileSystem fs = UnderFileSystemRegistry.create(path, ufsConf);
        cachedFs = mUnderFileSystemMap.putIfAbsent(key, fs);
        if (cachedFs == null) {
          return fs;
        }
        try {
          fs.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return cachedFs;
      }

      void clear() {
        mUnderFileSystemMap.clear();
      }
    }

    /**
     * The key of the UFS cache.
     */
    private static class Key {
      private final String mScheme;
      private final String mAuthority;

      Key(AlluxioURI uri) {
        mScheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
        mAuthority = uri.getAuthority() == null ? "" : uri.getAuthority().toLowerCase();
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(mScheme, mAuthority);
      }

      @Override
      public boolean equals(Object object) {
        if (object == this) {
          return true;
        }

        if (!(object instanceof Key)) {
          return false;
        }

        Key that = (Key) object;
        return Objects.equal(mScheme, that.mScheme)
            && Objects.equal(mAuthority, that.mAuthority);
      }

      @Override
      public String toString() {
        return mScheme + "://" + mAuthority;
      }
    }

    /**
     * Clears the under file system cache.
     */
    public static void clearCache() {
      UFS_CACHE.clear();
    }

    /**
     * Gets the UnderFileSystem instance according to its schema.
     *
     * @param path the file path storing over the ufs
     * @return instance of the under layer file system
     */
    public static UnderFileSystem get(String path) {
      return get(path, null);
    }

    /**
     * Gets the UnderFileSystem instance according to its scheme and configuration.
     *
     * @param path the file path storing over the ufs
     * @param ufsConf the configuration object for ufs only
     * @return instance of the under layer file system
     */
    public static UnderFileSystem get(String path, Object ufsConf) {
      Preconditions.checkArgument(path != null, "path may not be null");

      return UFS_CACHE.get(path, ufsConf);
    }
  }

  /**
   * The different types of space indicate the total space, the free space and the space used in the
   * under file system.
   */
  enum SpaceType {

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
    SPACE_USED(2),
    ;

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
   * Closes this under file system.
   *
   * @throws IOException if a non-Alluxio error occurs
   */
  void close() throws IOException;

  /**
   * Configures and updates the properties. For instance, this method can add new properties or
   * modify existing properties specified through {@link #setProperties(Map)}.
   *
   * The default implementation is a no-op. This should be overridden if a subclass needs
   * additional functionality.
   * @throws IOException if an error occurs during configuration
   */
  void configureProperties() throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given master host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param hostname the host that wants to connect to the under file system
   * @throws IOException if a non-Alluxio error occurs
   */
  void connectFromMaster(String hostname) throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given worker host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param hostname the host that wants to connect to the under file system
   * @throws IOException if a non-Alluxio error occurs
   */
  void connectFromWorker(String hostname) throws IOException;

  /**
   * Creates a file in the under file system with the indicated name.
   *
   * @param path the file name
   * @return A {@code OutputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  OutputStream create(String path) throws IOException;

  /**
   * Creates a file in the under file system with the specified {@link CreateOptions}.
   * Implementations should make sure that the path under creation appears in listings only
   * after a successful close and that contents are written in its entirety or not at all.
   *
   * @param path the file name
   * @param options the options for create
   * @return A {@code OutputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  OutputStream create(String path, CreateOptions options) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name non-recursively.
   *
   * @param path of the directory to delete
   * @return true if directory was found and deleted, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean deleteDirectory(String path) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name.
   *
   * @param path of the directory to delete
   * @param options for directory delete semantics
   * @return true if directory was found and deleted, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean deleteDirectory(String path, DeleteOptions options) throws IOException;

  /**
   * Deletes a file from the under file system with the indicated name.
   *
   * @param path of the file to delete
   * @return true if file was found and deleted, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean deleteFile(String path) throws IOException;

  /**
   * Gets the block size of a file in under file system, in bytes.
   *
   * @param path the file name
   * @return the block size in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  long getBlockSizeByte(String path) throws IOException;

  /**
   * Gets the configuration object for UnderFileSystem.
   *
   * @return configuration object used for concrete ufs instance
   */
  Object getConf();

  /**
   * Gets the list of locations of the indicated path.
   *
   * @param path the file name
   * @return The list of locations
   * @throws IOException if a non-Alluxio error occurs
   */
  List<String> getFileLocations(String path) throws IOException;

  /**
   * Gets the list of locations of the indicated path given options.
   *
   * @param path the file name
   * @param options method options
   * @return The list of locations
   * @throws IOException if a non-Alluxio error occurs
   */
  List<String> getFileLocations(String path, FileLocationOptions options) throws IOException;

  /**
   * Gets the file size in bytes.
   *
   * @param path the file name
   * @return the file size in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  long getFileSize(String path) throws IOException;

  /**
   * Gets the group of the given path. An empty implementation should be provided if not supported.
   *
   * @param path the path of the file
   * @return the group of the file
   * @throws IOException if a non-Alluxio error occurs
   */
  String getGroup(String path) throws IOException;

  /**
   * Gets the mode of the given path in short format, e.g 0700. An empty implementation should
   * be provided if not supported.
   *
   * @param path the path of the file
   * @return the mode of the file
   * @throws IOException if a non-Alluxio error occurs
   */
  short getMode(String path) throws IOException;

  /**
   * Gets the UTC time of when the indicated path was modified recently in ms.
   *
   * @param path the file name
   * @return modification time in milliseconds
   * @throws IOException if a non-Alluxio error occurs
   */
  long getModificationTimeMs(String path) throws IOException;

  /**
   * Gets the owner of the given path. An empty implementation should be provided if not supported.
   *
   * @param path the path of the file
   * @return the owner of the file
   * @throws IOException if a non-Alluxio error occurs
   */
  String getOwner(String path) throws IOException;

  /**
   * @return the property map for this {@link UnderFileSystem}
   */
  Map<String, String> getProperties();

  /**
   * Queries the under file system about the space of the indicated path (e.g., space left, space
   * used and etc).
   *
   * @param path the path to query
   * @param type the type of queries
   * @return The space in bytes
   * @throws IOException if a non-Alluxio error occurs
   */
  long getSpace(String path, SpaceType type) throws IOException;

  /**
   * Returns the name of the under filesystem implementation.
   *
   * The name should be lowercase and not include any spaces, e.g. "hdfs", "s3".
   *
   * @return name of the under filesystem implementation
   */
  String getUnderFSType();

  /**
   * Checks if a directory exists in under file system.
   *
   * @param path the absolute directory path
   * @return true if the path exists and is a directory, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean isDirectory(String path) throws IOException;

  /**
   * Checks if a file exists in under file system.
   *
   * @param path the absolute file path
   * @return true if the path exists and is a file, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean isFile(String path) throws IOException;

  /**
   * Returns an array of statuses of the files and directories in the directory denoted by this
   * abstract pathname.
   *
   * <p>
   * If this abstract pathname does not denote a directory, then this method returns {@code null}.
   * Otherwise an array of statuses is returned, one for each file or directory in the directory.
   * Names denoting the directory itself and the directory's parent directory are not included in
   * the result. Each string is a file name rather than a complete path.
   *
   * <p>
   * There is no guarantee that the name strings in the resulting array will appear in any specific
   * order; they are not, in particular, guaranteed to appear in alphabetical order.
   *
   * @param path the abstract pathname to list
   * @return An array with the statuses of the files and directories in the directory denoted by
   *         this abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException if a non-Alluxio error occurs
   */
  UnderFileStatus[] listStatus(String path) throws IOException;

  /**
   * Returns an array of statuses of the files and directories in the directory denoted by this
   * abstract pathname, with options.
   *
   * <p>
   * If this abstract pathname does not denote a directory, then this method returns {@code null}.
   * Otherwise an array of statuses is returned, one for each file or directory. Names denoting the
   * directory itself and the directory's parent directory are not included in the result. Each
   * string is a path relative to the given directory.
   *
   * <p>
   * There is no guarantee that the name strings in the resulting array will appear in any specific
   * order; they are not, in particular, guaranteed to appear in alphabetical order.
   *
   * @param path the abstract pathname to list
   * @param options for list directory
   * @return An array of statuses naming the files and directories in the directory denoted by this
   *         abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException if a non-Alluxio error occurs
   */
  UnderFileStatus[] listStatus(String path, ListOptions options) throws IOException;

  /**
   * Creates the directory named by this abstract pathname. If the folder already exists, the method
   * returns false. The method creates any necessary but nonexistent parent directories.
   *
   * @param path the folder to create
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean mkdirs(String path) throws IOException;

  /**
   * Creates the directory named by this abstract pathname, with specified
   * {@link MkdirsOptions}. If the folder already exists, the method returns false.
   *
   * @param path the folder to create
   * @param options the options for mkdirs
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean mkdirs(String path, MkdirsOptions options) throws IOException;

  /**
   * Opens an {@link InputStream} at the indicated path.
   *
   * @param path the file name
   * @return The {@code InputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  InputStream open(String path) throws IOException;

  /**
   * Opens an {@link InputStream} at the indicated path.
   *
   * @param path the file name
   * @param options to open input stream
   * @return The {@code InputStream} object
   * @throws IOException if a non-Alluxio error occurs
   */
  InputStream open(String path, OpenOptions options) throws IOException;

  /**
   * Renames a directory from {@code src} to {@code dst} in under file system.
   *
   * @param src the source directory path
   * @param dst the destination directory path
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean renameDirectory(String src, String dst) throws IOException;

  /**
   * Renames a file from {@code src} to {@code dst} in under file system.
   *
   * @param src the source file path
   * @param dst the destination file path
   * @return true if succeed, false otherwise
   * @throws IOException if a non-Alluxio error occurs
   */
  boolean renameFile(String src, String dst) throws IOException;

  /**
   * Returns an {@link AlluxioURI} representation for the {@link UnderFileSystem} given a base
   * UFS URI, and the Alluxio path from the base.
   *
   * The default implementation simply concatenates the path to the base URI. This should be
   * overridden if a subclass needs alternate functionality.
   *
   * @param ufsBaseUri the base {@link AlluxioURI} in the ufs
   * @param alluxioPath the path in Alluxio from the given base
   * @return the UFS {@link AlluxioURI} representing the Alluxio path
   */
  AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath);

  /**
   * Sets the configuration object for UnderFileSystem. The conf object is understood by the
   * concrete underfs's implementation.
   *
   * @param conf the configuration object accepted by ufs
   */
  void setConf(Object conf);

  /**
   * Sets the user and group of the given path. An empty implementation should be provided if
   * unsupported.
   *
   * @param path the path of the file
   * @param owner the new owner to set, unchanged if null
   * @param group the new group to set, unchanged if null
   * @throws IOException if a non-Alluxio error occurs
   */
  void setOwner(String path, String owner, String group) throws IOException;

  /**
   * Sets the properties for this {@link UnderFileSystem}.
   *
   * @param properties a {@link Map} of property names to values
   */
  void setProperties(Map<String, String> properties);

  /**
   * Changes posix file mode.
   *
   * @param path the path of the file
   * @param mode the mode to set in short format, e.g. 0777
   * @throws IOException if a non-Alluxio error occurs
   */
  void setMode(String path, short mode) throws IOException;

  /**
   * Whether this type of UFS supports flush.
   *
   * @return true if this type of UFS supports flush, false otherwise
   */
  boolean supportsFlush();
}
