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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system
 */
@ThreadSafe
// TODO(adit); API calls should use a URI instead of a String wherever appropriate
public interface UnderFileSystem extends Closeable {

  /**
   * The factory for the {@link UnderFileSystem}.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);

    private Factory() {} // prevent instantiation

    /**
     * Creates the {@link UnderFileSystem} instance according to its UFS path. This method should
     * only be used for journal operations and tests.
     *
     * @param path the file path storing over the ufs
     * @return instance of the under layer file system
     */
    public static UnderFileSystem create(String path) {
      return create(path, UnderFileSystemConfiguration.defaults());
    }

    /**
     * Creates the {@link UnderFileSystem} instance according to its UFS path. This method should
     * only be used for journal operations and tests.
     *
     * @param path journal path in ufs
     * @return the instance of under file system for Alluxio journal directory
     */
    public static UnderFileSystem create(URI path) {
      return create(path.toString());
    }

    /**
     * Creates a client for operations involved with the under file system. An
     * {@link IllegalArgumentException} is thrown if there is no under file system for the given
     * path or if no under file system could successfully be created.
     *
     * @param path path
     * @param ufsConf optional configuration object for the UFS, may be null
     * @return client for the under file system
     */
    public static UnderFileSystem create(String path, UnderFileSystemConfiguration ufsConf) {
      // Try to obtain the appropriate factory
      List<UnderFileSystemFactory> factories = UnderFileSystemFactoryRegistry.findAll(path);
      if (factories.isEmpty()) {
        throw new IllegalArgumentException("No Under File System Factory found for: " + path);
      }

      List<Throwable> errors = new ArrayList<>();
      for (UnderFileSystemFactory factory : factories) {
        try {
          // Use the factory to create the actual client for the Under File System
          return new UnderFileSystemWithLogging(factory.create(path, ufsConf));
        } catch (Throwable e) {
          // Catching Throwable rather than Exception to catch service loading errors
          errors.add(e);
          LOG.warn("Failed to create UnderFileSystem by factory {}: {}", factory, e.getMessage());
        }
      }

      // If we reach here no factories were able to successfully create for this path likely due to
      // missing configuration since if we reached here at least some factories claimed to support
      // the path
      // Need to collate the errors
      IllegalArgumentException e = new IllegalArgumentException(
          String.format("Unable to create an UnderFileSystem instance for path: %s", path));
      for (Throwable t : errors) {
        e.addSuppressed(t);
      }
      throw e;
    }

    /**
     * @return the instance of under file system for Alluxio root directory
     */
    public static UnderFileSystem createForRoot() {
      String ufsRoot = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
      boolean readOnly = Configuration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY);
      boolean shared = Configuration.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_SHARED);
      Map<String, String> ufsConf =
          Configuration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
      return create(ufsRoot, UnderFileSystemConfiguration.defaults().setReadOnly(readOnly)
          .setShared(shared).setUserSpecifiedConf(ufsConf));
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
   */
  void close() throws IOException;

  /**
   * Takes any necessary actions required to establish a connection to the under file system from
   * the given master host e.g. logging in
   * <p>
   * Depending on the implementation this may be a no-op
   * </p>
   *
   * @param hostname the host that wants to connect to the under file system
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
   */
  void connectFromWorker(String hostname) throws IOException;

  /**
   * Creates a file in the under file system with the indicated name. Parents directories will be
   * created recursively.
   *
   * @param path the file name
   * @return A {@code OutputStream} object
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
   */
  OutputStream create(String path, CreateOptions options) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name non-recursively. A
   * non-recursive delete is successful only if the directory is empty.
   *
   * @param path of the directory to delete
   * @return true if directory was found and deleted, false otherwise
   */
  boolean deleteDirectory(String path) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name.
   *
   * @param path of the directory to delete
   * @param options for directory delete semantics
   * @return true if directory was found and deleted, false otherwise
   */
  boolean deleteDirectory(String path, DeleteOptions options) throws IOException;

  /**
   * Deletes a file from the under file system with the indicated name.
   *
   * @param path of the file to delete
   * @return true if file was found and deleted, false otherwise
   */
  boolean deleteFile(String path) throws IOException;

  /**
   * Checks if a file or directory exists in under file system.
   *
   * @param path the absolute path
   * @return true if the path exists, false otherwise
   */
  boolean exists(String path) throws IOException;

  /**
   * Gets the block size of a file in under file system, in bytes.
   *
   * @param path the file name
   * @return the block size in bytes
   */
  long getBlockSizeByte(String path) throws IOException;

  /**
   * Gets the directory status.
   *
   * @param path the file name
   * @return the directory status
   */
  UfsDirectoryStatus getDirectoryStatus(String path) throws IOException;

  /**
   * Gets the list of locations of the indicated path.
   *
   * @param path the file name
   * @return The list of locations
   */
  List<String> getFileLocations(String path) throws IOException;

  /**
   * Gets the list of locations of the indicated path given options.
   *
   * @param path the file name
   * @param options method options
   * @return The list of locations
   */
  List<String> getFileLocations(String path, FileLocationOptions options) throws IOException;

  /**
   * Gets the file status.
   *
   * @param path the file name
   * @return the file status
   */
  UfsFileStatus getFileStatus(String path) throws IOException;

  /**
   * Queries the under file system about the space of the indicated path (e.g., space left, space
   * used and etc).
   *
   * @param path the path to query
   * @param type the type of queries
   * @return The space in bytes
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
   */
  boolean isDirectory(String path) throws IOException;

  /**
   * Checks if a file exists in under file system.
   *
   * @param path the absolute file path
   * @return true if the path exists and is a file, false otherwise
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
   */
  UfsStatus[] listStatus(String path) throws IOException;

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
   */
  UfsStatus[] listStatus(String path, ListOptions options) throws IOException;

  /**
   * Creates the directory named by this abstract pathname. If the folder already exists, the method
   * returns false. The method creates any necessary but nonexistent parent directories.
   *
   * @param path the folder to create
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   */
  boolean mkdirs(String path) throws IOException;

  /**
   * Creates the directory named by this abstract pathname, with specified
   * {@link MkdirsOptions}. If the folder already exists, the method returns false.
   *
   * @param path the folder to create
   * @param options the options for mkdirs
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   */
  boolean mkdirs(String path, MkdirsOptions options) throws IOException;

  /**
   * Opens an {@link InputStream} for a file in under filesystem at the indicated path.
   *
   * @param path the file name
   * @return The {@code InputStream} object
   */
  InputStream open(String path) throws IOException;

  /**
   * Opens an {@link InputStream} for a file in under filesystem at the indicated path.
   *
   * @param path the file name
   * @param options to open input stream
   * @return The {@code InputStream} object
   */
  InputStream open(String path, OpenOptions options) throws IOException;

  /**
   * Renames a directory from {@code src} to {@code dst} in under file system.
   *
   * @param src the source directory path
   * @param dst the destination directory path
   * @return true if succeed, false otherwise
   */
  boolean renameDirectory(String src, String dst) throws IOException;

  /**
   * Renames a file from {@code src} to {@code dst} in under file system.
   *
   * @param src the source file path
   * @param dst the destination file path
   * @return true if succeed, false otherwise
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
   * Changes posix file mode.
   *
   * @param path the path of the file
   * @param mode the mode to set in short format, e.g. 0777
   */
  void setMode(String path, short mode) throws IOException;

  /**
   * Sets the user and group of the given path. An empty implementation should be provided if
   * unsupported.
   *
   * @param path the path of the file
   * @param owner the new owner to set, unchanged if null
   * @param group the new group to set, unchanged if null
   */
  void setOwner(String path, String owner, String group) throws IOException;

  /**
   * Whether this type of UFS supports flush.
   *
   * @return true if this type of UFS supports flush, false otherwise
   */
  boolean supportsFlush();
}
