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
import alluxio.PositionReader;
import alluxio.SyncInfo;
import alluxio.annotation.PublicApi;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.recorder.Recorder;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.GetStatusOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio stores data into an under layer file system. Any file system implementing this interface
 * can be a valid under layer file system.
 *
 * There are two sets of APIs in the under file system:
 * (1) normal operations (e.g. create, renameFile, deleteFile)
 * (2) operations deal with the eventual consistency issue
 * (e.g. createNonexistingFile, renameRenamableFile)
 * When confirmed by Alluxio metadata that an operation should succeed but may fail because of the
 * under filesystem eventual consistency issue, use the second set of APIs.
 */
@PublicApi
@ThreadSafe
// TODO(adit); API calls should use a URI instead of a String wherever appropriate
public interface UnderFileSystem extends Closeable, AsyncUfsClient {
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
     * @param path journal path in ufs
     * @param conf the configuration object w/o mount specific options
     * @return the instance of under file system for Alluxio journal directory
     */
    public static UnderFileSystem create(String path, AlluxioConfiguration conf) {
      return create(path, UnderFileSystemConfiguration.defaults(conf));
    }

    /**
     * Creates a client for operations involved with the under file system. An
     * {@link IllegalArgumentException} is thrown if there is no under file system for the given
     * path or if no under file system could successfully be created.
     *
     * @param path path
     * @param ufsConf configuration object for the UFS
     * @return client for the under file system
     */
    public static UnderFileSystem create(String path, UnderFileSystemConfiguration ufsConf) {
      return createWithRecorder(path, ufsConf, Recorder.noopRecorder());
    }

    /**
     * Creates a client for operations involved with the under file system and record the
     * execution process.
     * An {@link IllegalArgumentException} is thrown if there is no under file system for the given
     * path or if no under file system could successfully be created.
     *
     * @param path path
     * @param ufsConf configuration object for the UFS
     * @param recorder recorder used to record the detailed execution process
     * @return client for the under file system
     */
    public static UnderFileSystem createWithRecorder(String path,
        UnderFileSystemConfiguration ufsConf, Recorder recorder) {
      // Try to obtain the appropriate factory
      List<UnderFileSystemFactory> factories =
          UnderFileSystemFactoryRegistry.findAllWithRecorder(path, ufsConf, recorder);
      if (factories.isEmpty()) {
        throw new IllegalArgumentException("No Under File System Factory found for: " + path);
      }

      List<Throwable> errors = new ArrayList<>();
      for (UnderFileSystemFactory factory : factories) {
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
        try {
          // Reflection may be invoked during UFS creation on service loading which uses context
          // classloader by default. Stashing the context classloader on creation and switch it back
          // when creation is done.
          recorder.record(
              "Trying to create UFS from factory {} of version {} for path {} with ClassLoader {}",
              factory.getClass().getSimpleName(),
              factory.getVersion(),
              path,
              factory.getClass().getClassLoader().getClass().getSimpleName());
          Thread.currentThread().setContextClassLoader(factory.getClass().getClassLoader());
          UnderFileSystem underFileSystem =
              new UnderFileSystemWithLogging(path, factory.create(path, ufsConf), ufsConf);
          // Use the factory to create the actual client for the Under File System
          recorder.record("UFS created with factory {}",
              factory.getClass().getSimpleName());
          return underFileSystem;
        } catch (Throwable e) {
          // Catching Throwable rather than Exception to catch service loading errors
          errors.add(e);
          String errorMsg = String.format(
              "Failed to create UnderFileSystem by factory %s: %s",
              factory.getClass().getSimpleName(), e);
          recorder.record(errorMsg);
          LOG.warn(errorMsg);
        } finally {
          Thread.currentThread().setContextClassLoader(previousClassLoader);
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
     * @param conf configuration
     * @return the instance of under file system for Alluxio root directory
     */
    public static UnderFileSystem createForRoot(AlluxioConfiguration conf) {
      String ufsRoot = conf.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
      boolean readOnly = conf.getBoolean(PropertyKey.MASTER_MOUNT_TABLE_ROOT_READONLY);
      Map<String, Object> ufsConf =
          conf.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
      return create(ufsRoot, new UnderFileSystemConfiguration(conf, readOnly)
          .createMountSpecificConf(ufsConf));
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
   * Cleans up the under file system. If any data or files are created
   * and not completed/aborted correctly in normal ways, they should be cleaned in this method.
   */
  void cleanup() throws IOException;

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
   * Creates a file in the under file system with the indicated name.
   *
   * Similar to {@link #create(String)} but
   * deals with the delete-then-create eventual consistency issue.
   *
   * @param path the file name
   * @return A {@code OutputStream} object
   */
  OutputStream createNonexistingFile(String path) throws IOException;

  /**
   * Creates a file in the under file system with the specified {@link CreateOptions}.
   *
   * Similar to {@link #create(String, CreateOptions)} but
   * deals with the delete-then-create eventual consistency issue.
   *
   * @param path the file name
   * @param options the options for create
   * @return A {@code OutputStream} object
   */
  OutputStream createNonexistingFile(String path, CreateOptions options) throws IOException;

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
   * Deletes a directory from the under file system.
   *
   * Similar to {@link #deleteDirectory(String)} but
   * deals with the create-delete eventual consistency issue.
   *
   * @param path of the directory to delete
   * @return true if directory was found and deleted, false otherwise
   */
  boolean deleteExistingDirectory(String path) throws IOException;

  /**
   * Deletes a directory from the under file system with the indicated name.
   *
   * Similar to {@link #deleteDirectory(String, DeleteOptions)} but
   * deals with the create-then-delete eventual consistency issue.
   *
   * @param path of the directory to delete
   * @param options for directory delete semantics
   * @return true if directory was found and deleted, false otherwise
   */
  boolean deleteExistingDirectory(String path, DeleteOptions options) throws IOException;

  /**
   * Deletes a file from the under file system with the indicated name.
   *
   * @param path of the file to delete
   * @return true if file was found and deleted, false otherwise
   */
  boolean deleteFile(String path) throws IOException;

  /**
   * Deletes a file from the under file system with the indicated name.
   *
   * Similar to {@link #deleteFile(String)} but
   * deals with the create-then-delete eventual consistency issue.
   *
   * @param path of the file to delete
   * @return true if file was found and deleted, false otherwise
   */
  boolean deleteExistingFile(String path) throws IOException;

  /**
   * Checks if a file or directory exists in under file system.
   *
   * @param path the absolute path
   * @return true if the path exists, false otherwise
   */
  boolean exists(String path) throws IOException;

  /**
   * Gets the ACL and the Default ACL of a file or directory in under file system.
   *
   * @param path the path to the file or directory
   * @return the access control list, along with a Default ACL if it is a directory
   *         return null if ACL is unsupported or disabled
   * @throws IOException if ACL is supported and enabled but cannot be retrieved
   */
  @Nullable
  Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path) throws IOException;

  /**
   * Gets the block size of a file in under file system, in bytes.
   *
   * @deprecated block size should be returned as part of the {@link UfsFileStatus} from
   * {@link #getFileStatus(String)} or {@link #getExistingFileStatus(String)}
   *
   * @param path the file name
   * @return the block size in bytes
   */
  @Deprecated
  long getBlockSizeByte(String path) throws IOException;

  /**
   * Gets the under file system configuration.
   *
   * @return the configuration
   */
  default AlluxioConfiguration getConfiguration() throws IOException {
    return UnderFileSystemConfiguration.emptyConfig();
  }

  /**
   * Gets the directory status. The caller must already know the path is a directory. This method
   * will throw an exception if the path exists, but is a file.
   *
   * @param path the path to the directory
   * @return the directory status
   * @throws FileNotFoundException when the path does not exist
   */
  UfsDirectoryStatus getDirectoryStatus(String path) throws IOException;

  /**
   * Gets the directory status.
   *
   * Similar to {@link #getDirectoryStatus(String)} but
   * deals with the write-then-get-status eventual consistency issue.
   *
   * @param path the path to the directory
   * @return the directory status
   */
  UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException;

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
   * Gets the file status. The caller must already know the path is a file. This method will
   * throw an exception if the path exists, but is a directory.
   *
   * @param path the path to the file
   * @return the file status
   * @throws FileNotFoundException when the path does not exist
   */
  default UfsFileStatus getFileStatus(String path) throws IOException {
    return getFileStatus(path, GetStatusOptions.defaults());
  }

  /**
   * Gets the file status. The caller must already know the path is a file. This method will
   * throw an exception if the path exists, but is a directory.
   *
   * @param path the path to the file
   * @param options method options
   * @return the file status
   * @throws FileNotFoundException when the path does not exist
   */
  UfsFileStatus getFileStatus(String path, GetStatusOptions options) throws IOException;

  /**
   * Gets the file status.
   *
   * Similar to {@link #getFileStatus(String)} but
   * deals with the write-then-get-status eventual consistency issue.
   *
   * @param path the path to the file
   * @return the file status
   */
  UfsFileStatus getExistingFileStatus(String path) throws IOException;

  /**
   * Computes and returns a fingerprint for the path. The fingerprint is used to determine if two
   * UFS files are identical. The fingerprint must be deterministic, and must not change if a
   * file is only renamed (identical content and permissions). Returns
   * {@link alluxio.Constants#INVALID_UFS_FINGERPRINT} if there is any error.
   *
   * @deprecated
   * @param path the path to compute the fingerprint for
   * @return the string representing the fingerprint
   */
  @Deprecated
  String getFingerprint(String path);

  /**
   * Computes and returns a fingerprint for the path. The fingerprint is used to determine if two
   * UFS files are identical. The fingerprint must be deterministic, and must not change if a
   * file is only renamed (identical content and permissions). Returns
   * {@link Fingerprint#INVALID_FINGERPRINT} if there is any error.
   *
   * The default implementation relies on {@link #getFingerprint(String)} and there is one extra
   * parsing. This default implementation is mainly for backward compatibility.
   * Override this for performance.
   *
   * @param path the path to compute the fingerprint for
   * @return the string representing the fingerprint
   */
  Fingerprint getParsedFingerprint(String path);

  /**
   * Same as {@link #getParsedFingerprint(String)} except, will use the given content hash
   * as the {@link alluxio.underfs.Fingerprint.Tag#CONTENT_HASH} field of the fingerprint
   * if non-null. This is intended to be used when the file is already in Alluxio and
   * a fingerprint is being created based on that file where the content hash has already
   * been computed.
   * @param path the path to compute the fingerprint for
   * @param contentHash is used as the {@link alluxio.underfs.Fingerprint.Tag#CONTENT_HASH}
   *                    field when creating the fingerprint.
   * @return the string representing the fingerprint
   */
  Fingerprint getParsedFingerprint(String path, @Nullable String contentHash);

  /**
   * An {@link UnderFileSystem} may be composed of one or more "physical UFS"s. This method is used
   * to determine the operation mode based on the physical UFS operation modes. For example, if this
   * {@link UnderFileSystem} is composed of physical UFS hdfs://ns1/ and hdfs://ns2/ with read
   * operations split b/w the two, with physicalUfsState{hdfs://ns1/:NO_ACCESS,
   * hdfs://ns2/:READ_WRITE} this method can return READ_ONLY to allow reads to proceed from
   * hdfs://ns2/.
   *
   * @param physicalUfsState the state of physical UFSs for this {@link UnderFileSystem}; keys are
   *        expected to be normalized (ending with /)
   * @return the desired operation mode for this UFS
   */
  UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState);

  /**
   * An {@link UnderFileSystem} may be composed of one or more "physical UFS"s. This method
   * returns all underlying physical stores; normalized with only scheme and authority.
   *
   * @return physical UFSs this {@link UnderFileSystem} is composed of
   */
  List<String> getPhysicalStores();

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
   * Gets the file or directory status. The caller does not need to know if the path is a file or
   * directory. This method will determine the path type, and will return the appropriate status.
   *
   * @param path the path to get the status
   * @return the file or directory status
   * @throws FileNotFoundException when the path does not exist
   */
  default UfsStatus getStatus(String path) throws IOException {
    return getStatus(path, GetStatusOptions.defaults());
  }

  /**
   * Gets the file or directory status. The caller does not need to know if the path is a file or
   * directory. This method will determine the path type, and will return the appropriate status.
   *
   * @param path the path to get the status
   * @param options method options
   * @return the file or directory status
   * @throws FileNotFoundException when the path does not exist
   */
  UfsStatus getStatus(String path, GetStatusOptions options) throws IOException;

  /**
   * Gets the file or directory status.
   *
   * Similar to {@link #getStatus(String)} but
   * deals with the write-then-get-status eventual consistency issue.
   *
   * @param path the path to get the status
   * @return the file or directory status
   */
  UfsStatus getExistingStatus(String path) throws IOException;

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
   * Checks if a directory exists in under file system.
   *
   * Similar to {@link #isDirectory(String)} but
   * deals with the write-then-list eventual consistency issue.
   *
   * @param path the absolute directory path
   * @return true if the path exists and is a directory, false otherwise
   */
  boolean isExistingDirectory(String path) throws IOException;

  /**
   * Checks if a file exists in under file system.
   *
   * @param path the absolute file path
   * @return true if the path exists and is a file, false otherwise
   */
  boolean isFile(String path) throws IOException;

  /**
   * @return true if under storage is an object store, false otherwise
   */
  boolean isObjectStorage();

  /**
   * Denotes if the under storage supports seeking. Note, the under file system subclass that
   * returns true for this method should return the input stream extending
   * {@link SeekableUnderFileInputStream} in the {@link #open(String, OpenOptions)} method.
   *
   * @return true if under storage is seekable, false otherwise
   */
  boolean isSeekable();

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
  @Nullable
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
  @Nullable
  UfsStatus[] listStatus(String path, ListOptions options) throws IOException;

  /**
   * Lists the ufs statuses iteratively.
   *
   * @param path the abstract pathname to list
   * @param options for list directory
   * @param startAfter the start after token
   * @param batchSize the batch size
   * @return An iterator of ufs status. Returns
   *  {@code null} if this abstract pathname does not denote a directory.
   */
  @Nullable
  Iterator<UfsStatus> listStatusIterable(
      String path, ListOptions options, String startAfter, int batchSize) throws IOException;

  /**
   * Returns an array of statuses of the files and directories in the directory denoted by this
   * abstract pathname, with options.
   *
   * <p>
   * If this abstract pathname does not denote a directory, meaning it might be a file,
   * then this method would try to call getStatus on the path.
   * An array of statuses is returned, one for each file or directory. Names denoting the
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
   *         abstract pathname. The array will be empty if the directory is empty.
   *         Or And array of single file if given path is a file.
   *         Returns {@code Optional.empty()} if this abstract pathname does not denote a valid
   *         directory or file not found.
   */
  default Optional<UfsStatus[]> listStatuses(String path, ListOptions options) throws IOException {
    UfsStatus[] statuses;
    statuses = listStatus(path, options);
    if (statuses == null) {
      // If empty, the request path might be a regular file/object. Let's retry getStatus().
      try {
        UfsStatus status = getStatus(path);
        status.setName("");
        statuses = new UfsStatus[1];
        statuses[0] = status;
      } catch (FileNotFoundException e) {
        // statuses already null
      }
    }
    if (statuses == null) {
      return Optional.empty();
    }
    return Optional.of(statuses);
  }

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
   * Opens an {@link InputStream} for a file in under filesystem at the indicated path.
   *
   * Similar to {@link #open(String)} but
   * deals with the write-then-read eventual consistency issue.
   *
   * @param path the file name
   * @return The {@code InputStream} object
   */
  InputStream openExistingFile(String path) throws IOException;

  /**
   * Opens an {@link InputStream} for a file in under filesystem at the indicated path.
   *
   * Similar to {@link #open(String, OpenOptions)} but
   * deals with the write-then-read eventual consistency issue.
   *
   * @param path the file name
   * @param options to open input stream
   * @return The {@code InputStream} object
   */
  InputStream openExistingFile(String path, OpenOptions options) throws IOException;

  /**
   * Opens a file for position read.
   *
   * @param path the path to read
   * @param fileLength the file length
   * @return the position reader
   */
  // TODO(lu) how to use UFS IO manager to control UFS IO
  PositionReader openPositionRead(String path, long fileLength);

  /**
   * Renames a directory from {@code src} to {@code dst} in under file system.
   *
   * @param src the source directory path
   * @param dst the destination directory path
   * @return true if succeed, false otherwise
   */
  boolean renameDirectory(String src, String dst) throws IOException;

  /**
   * Renames a directory from {@code src} to {@code dst} in under file system.
   *
   * Similar to {@link #renameDirectory(String, String)} but
   * deals with the write-src-then-rename and delete-dst-then-rename eventual consistency issue.
   *
   * @param src the source directory path
   * @param dst the destination directory path
   * @return true if succeed, false otherwise
   */
  boolean renameRenamableDirectory(String src, String dst) throws IOException;

  /**
   * Renames a file from {@code src} to {@code dst} in under file system.
   *
   * @param src the source file path
   * @param dst the destination file path
   * @return true if succeed, false otherwise
   */
  boolean renameFile(String src, String dst) throws IOException;

  /**
   * Renames a file from {@code src} to {@code dst} in under file system.
   *
   * Similar to {@link #renameFile(String, String)} but
   * deals with the write-src-then-rename and delete-dst-then-rename eventual consistency issue.
   *
   * @param src the source file path
   * @param dst the destination file path
   * @return true if succeed, false otherwise
   */
  boolean renameRenamableFile(String src, String dst) throws IOException;

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
   * Sets the access control list of a file or directory in under file system.
   * if the ufs does not support acls, this is a noop.
   * This will overwrite the ACL and defaultACL in the UFS.
   *
   * @param path the path to the file or directory
   * @param aclEntries the access control list + default acl represented in a list of acl entries
   */
  void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException;

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
  boolean supportsFlush() throws IOException;

  /**
   * Whether this type of UFS supports active sync.
   *
   * @return true if this type of UFS supports active sync, false otherwise
   */
  boolean supportsActiveSync();

  /**
   * Return the active sync info for the specified syncPoints.
   *
   * @return active sync info consisting of what changed for these sync points
   */
  SyncInfo getActiveSyncInfo() throws IOException;

  /**
   * Add Sync Point.
   *
   * @param uri ufs uri to start
   */
  void startSync(AlluxioURI uri) throws IOException;

  /**
   * Stop Sync Point.
   *
   * @param uri ufs uri to stop
   */
  void stopSync(AlluxioURI uri) throws IOException;

  /**
   * Start Active Sync.
   *
   * @param txId the transaction id to start receiving event
   * @return true if active sync started
   */
  boolean startActiveSyncPolling(long txId) throws IOException;

  /**
   * Stop Active Sync.
   *
   * @return true if active sync stopped
   */
  boolean stopActiveSyncPolling() throws IOException;

  /**
   * Gets a temporary token according to path, sid, effect and action.
   *
   * @param path the resource path
   * @param sid the user id
   * @param effect the effect
   * @param action the action to be done
   * @return the byte array of the temporary token
   */
  default byte[] getTemporaryToken(String path, String sid, String effect,
      alluxio.security.authorization.Mode.Bits action) throws IOException {
    throw new java.lang.UnsupportedOperationException("No implementation");
  }

  /**
   * Sets the userState to UFS instance.
   *
   * @param userState the userState
   */
  default void setTemporaryTokenRefreshContext(alluxio.security.user.UserState userState) {
    // noop
  }

  /**
   * Denotes if the under storage is in the temporary connection token mode.
   *
   * @return true if the under storage is in the temporary connection token mode
   */
  default boolean isTemporaryConnectionToken() {
    return false;
  }

  /**
   * Refreshes the old temporary token.
   *
   * @param oldTemporaryToken the old temporary token
   * @return the byte array of the temporary token
   */
  default byte[] refreshTemporaryToken(byte[] oldTemporaryToken) throws IOException {
    throw new java.lang.UnsupportedOperationException("No implementation");
  }

  /**
   * Whether this type of UFS supports Multipart upload.
   *
   * @return true if this type of UFS supports, false otherwise
   */
  default boolean supportUfsMultiPart() {
    return false;
  }

  /**
   * Initializes MultiPart Upload Task.
   *
   * @param path the path of the ufs file
   * @param options ufs options
   * @return the upload id of new Multipart task
   * @throws IOException
   */
  default String initObjectMultiPart(String path,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Uploads a part in the under file system with the indicated upload id and part number.
   *
   * @param path the path of the ufs file
   * @param uploadId the upload id of Multipart task
   * @param partNum the partNum to upload part
   * @param options ufs options
   * @return A {@code OutputStream} object
   * @throws IOException
   */
  default OutputStream uploadObjectMultiPart(String path, String uploadId, int partNum,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Uploads a part in the under file system with the indicated upload id and part number.
   *
   * @param path the path of the ufs file
   * @param uploadId the upload id of Multipart task
   * @param partNum the partNum to upload part
   * @param fileSize the content length
   * @param stream A {@code InputStream} Object to upload data
   * @param options ufs options
   * @return Etag of this part
   * @throws IOException e
   */
  default String uploadObjectMultiPart(String path, String uploadId, int partNum, long fileSize,
      InputStream stream,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Copy the data of a file in the ufs to be a part data with the indicated upload id
   * and part number.
   *
   * @param destPath  the path of the destination ufs file
   * @param srcPath   the path of the copy source ufs file
   * @param uploadId  the upload id of Multipart task
   * @param partNum   the partNum to upload part
   * @param firstByte the optional zero-based byte offset to begin copying the source object
   * @param lastByte  the optional zero-based byte offset to stop copying the source
   * @param options   ufs options
   * @return Etag of this part
   * @throws IOException
   */
  default String uploadPartCopy(String destPath, String srcPath, String uploadId, int partNum,
      @Nullable Long firstByte, @Nullable Long lastByte,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Completes a multipart Upload task in the under file system.
   *
   * @param path the path of the ufs file
   * @param uploadId the upload id of multipart task
   * @param etags the list of parts and their Etags
   * @param options ufs options
   * @return Etag of the completed file
   * @throws IOException
   */
  default String completeObjectMultiPart(String path, String uploadId,
      List<Pair<Integer, String>> etags,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Gets the list of uncompleted multipart upload task.
   *
   * @param options ufs options
   * @return list of multipart upload task
   * @throws IOException
   */
  default alluxio.underfs.response.ListMultipartUploadResult listMultipartUploads(
      alluxio.underfs.options.ListMultiPartOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Gets the list of part with the indicated file path and upload id.
   *
   * @param path the path of the ufs file
   * @param uploadId the upload id of multipart task
   * @param options ufs options
   * @return the list of part
   * @throws IOException
   */
  default List<alluxio.underfs.response.PartSummaryInfo> listUfsParts(String path, String uploadId,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }

  /**
   * Aborts a multipart upload task in the under file system.
   *
   * @param path the path of the ufs file
   * @param uploadId the upload id of multipart task
   * @param options ufs options
   * @throws IOException
   */
  default void abortUfsMultipartTask(String path, String uploadId,
      alluxio.underfs.options.MultipartUfsOptions options)
      throws IOException {
    // not support
    throw new IOException("not support");
  }
}
