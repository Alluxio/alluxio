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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.annotation.PublicApi;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.security.auth.Subject;

/**
 * Basic file system interface supporting metadata operations and data operations. Developers
 * should not implement this class but extend the default implementation provided by {@link
 * BaseFileSystem} instead. This ensures any new methods added to the interface will be provided
 * by the default implementation.
 */
@PublicApi
public interface FileSystem extends Closeable {

  /**
   * Factory for {@link FileSystem}. Calling any of the {@link Factory#get()} methods in this class
   * will attempt to return a cached instance of an Alluxio {@link FileSystem}. Using any of the
   * {@link Factory#create} methods will always guarantee returning a new FileSystem.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);
    private static final AtomicBoolean CONF_LOGGED = new AtomicBoolean(false);

    protected static final FileSystemCache FILESYSTEM_CACHE = new FileSystemCache();

    private Factory() {} // prevent instantiation

    /**
     * @return a FileSystem from the cache, creating a new one if it doesn't yet exist
     */
    public static FileSystem get() {
      return get(new Subject()); // Use empty subject
    }

    /**
     * Get a FileSystem from the cache with a given subject.
     *
     * @param subject The subject to use for security-related client operations
     * @return a FileSystem from the cache, creating a new one if it doesn't yet exist
     */
    public static FileSystem get(Subject subject) {
      return get(subject, new InstancedConfiguration(ConfigurationUtils.defaults()));
    }

    public static FileSystem get(Subject subject, AlluxioConfiguration conf) {
      Preconditions.checkNotNull(subject, "subject");
      // TODO(gpang): should this key use the UserState instead of subject?
      FileSystemCache.Key key =
          new FileSystemCache.Key(UserState.Factory.create(conf, subject).getSubject(), conf);
      return FILESYSTEM_CACHE.get(key);
    }

    /**
     * @param alluxioConf the configuration to utilize with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystem create(AlluxioConfiguration alluxioConf) {
      return create(FileSystemContext.create(alluxioConf));
    }

    /**
     * @param ctx the context with the subject and configuration to utilize with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystem create(ClientContext ctx) {
      return create(FileSystemContext.create(ctx));
    }

    /**
     * @param context the FileSystemContext to use with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystem create(FileSystemContext context) {
      AlluxioConfiguration conf = context.getClusterConf();
      if (LOG.isDebugEnabled() && !CONF_LOGGED.getAndSet(true)) {
        // Sort properties by name to keep output ordered.
        List<PropertyKey> keys = new ArrayList<>(conf.keySet());
        keys.sort(Comparator.comparing(PropertyKey::getName));
        for (PropertyKey key : keys) {
          String value = conf.getOrDefault(key, null);
          Source source = conf.getSource(key);
          LOG.debug("{}={} ({})", key.getName(), value, source);
        }
      }
      FileSystem fs = conf.getBoolean(PropertyKey.USER_METADATA_CACHE_ENABLED)
          ? new MetadataCachingBaseFileSystem(context) : new BaseFileSystem(context);
      // Enable local cache only for clients which have the property set.
      if (conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ENABLED)
          && CommonUtils.PROCESS_TYPE.get().equals(CommonUtils.ProcessType.CLIENT)) {
        try {
          CacheManager cacheManager = CacheManager.Factory.get(conf);
          return new LocalCacheFileSystem(cacheManager, fs, conf);
        } catch (IOException e) {
          LOG.error("Fallback without client caching: ", e);
        }
      }
      return fs;
    }
  }

  /**
   * If there are operations currently running and close is called concurrently the behavior is
   * undefined. After closing a FileSystem, any operations that are performed result in undefined
   * behavior.
   *
   * @return whether or not this FileSystem has been closed
   */
  boolean isClosed();

  /**
   * Checks access to a path.
   *
   * @param path the path of the directory to create in Alluxio space
   * @param options options to associate with this operation
   * @throws InvalidPathException if the path is invalid
   * @throws alluxio.exception.AccessControlException if the access is denied
   */
  void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #createDirectory(AlluxioURI, CreateDirectoryPOptions)} with
   * default options.
   *
   * @param path the path of the directory to create in Alluxio space
   * @throws FileAlreadyExistsException if there is already a file or directory at the given path
   * @throws InvalidPathException if the path is invalid
   */
  default void createDirectory(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    createDirectory(path, CreateDirectoryPOptions.getDefaultInstance());
  }

  /**
   * Creates a directory.
   *
   * @param path the path of the directory to create in Alluxio space
   * @param options options to associate with this operation
   * @throws FileAlreadyExistsException if there is already a file or directory at the given path
   * @throws InvalidPathException if the path is invalid
   */
  void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #createFile(AlluxioURI, CreateFilePOptions)} with default
   * options.
   *
   * @param path the path of the file to create in Alluxio space
   * @return a {@link FileOutStream} which will write data to the newly created file
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  default FileOutStream createFile(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return createFile(path, CreateFilePOptions.getDefaultInstance());
  }

  /**
   * Creates a file.
   *
   * @param path the path of the file to create in Alluxio space
   * @param options options to associate with this operation
   * @return a {@link FileOutStream} which will write data to the newly created file
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #delete(AlluxioURI, DeletePOptions)} with default options.
   *
   * @param path the path to delete in Alluxio space
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws DirectoryNotEmptyException if recursive is false and the path is a nonempty directory
   */
  default void delete(AlluxioURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    delete(path, DeletePOptions.getDefaultInstance());
  }

  /**
   * Deletes a file or a directory.
   *
   * @param path the path to delete in Alluxio space
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws DirectoryNotEmptyException if recursive is false and the path is a nonempty directory
   */
  void delete(AlluxioURI path, DeletePOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #exists(AlluxioURI, ExistsPOptions)} with default options.
   *
   * @param path the path in question
   * @return true if the path exists, false otherwise
   * @throws InvalidPathException if the path is invalid
   */
  default boolean exists(AlluxioURI path)
      throws InvalidPathException, IOException, AlluxioException {
    return exists(path, ExistsPOptions.getDefaultInstance());
  }

  /**
   * Checks whether a path exists in Alluxio space.
   *
   * @param path the path in question
   * @param options options to associate with this operation
   * @return true if the path exists, false otherwise
   * @throws InvalidPathException if the path is invalid
   */
  boolean exists(AlluxioURI path, ExistsPOptions options)
      throws InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #free(AlluxioURI, FreePOptions)} with default options.
   *
   * @param path the path to free in Alluxio space
   * @throws FileDoesNotExistException if the given path does not exist
   */
  default void free(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    free(path, FreePOptions.getDefaultInstance());
  }

  /**
   * Evicts any data under the given path from Alluxio space, but does not delete the data from the
   * UFS. The metadata will still be present in Alluxio space after this operation.
   *
   * @param path the path to free in Alluxio space
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   */
  void free(AlluxioURI path, FreePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Builds a list of {@link BlockLocationInfo} for the given file. Each list item contains a list
   * of {@link WorkerNetAddress} which allows a user to determine the physical location of a block
   * of the given file stored within Alluxio. In the case where data is stored in a UFS, but not in
   * Alluxio this function will only include a {@link WorkerNetAddress} if the block stored in the
   * UFS is co-located with an Alluxio worker.
   * However if there are no co-located Alluxio workers for the block, then the behavior is
   * controlled by the {@link PropertyKey#USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED} . If
   * this property is set to {@code true} then every Alluxio worker will be returned.
   * Blocks which are stored in the UFS and are *not* co-located with any Alluxio worker will return
   * an empty list. If the file block is within Alluxio *and* the UFS then this will only return
   * Alluxio workers which currently store the block.
   *
   * @param path the path to get block info for
   * @return a list of blocks with the workers whose hosts have the blocks. The blocks may not
   *         necessarily be stored in Alluxio. The blocks are returned in the order of their
   *         sequences in file.
   * @throws FileDoesNotExistException if the given path does not exist
   */
  List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * @return the configuration which the FileSystem is using to connect to Alluxio
   */
  AlluxioConfiguration getConf();

  /**
   * Convenience method for {@link #getStatus(AlluxioURI, GetStatusPOptions)} with default options.
   *
   * @param path the path to obtain information about
   * @return the {@link URIStatus} of the file
   * @throws FileDoesNotExistException if the path does not exist
   */
  default URIStatus getStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return getStatus(path, GetStatusPOptions.getDefaultInstance());
  }

  /**
   * Gets the {@link URIStatus} object that represents the metadata of an Alluxio path.
   *
   * @param path the path to obtain information about
   * @param options options to associate with this operation
   * @return the {@link URIStatus} of the file
   * @throws FileDoesNotExistException if the path does not exist
   */
  URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Performs a specific action on each {@code URIStatus} in the result of {@link #listStatus}.
   * This method is preferred when iterating over directories with a large number of files or
   * sub-directories inside. The caller can proceed with partial result without waiting for all
   * result returned.
   *
   * @param path the path to list information about
   * @param action action to apply on each {@code URIStatus}
   * @throws FileDoesNotExistException if the given path does not exist
   */
  default void iterateStatus(AlluxioURI path, Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    iterateStatus(path, ListStatusPOptions.getDefaultInstance(), action);
  }

  /**
   * Performs a specific action on each {@code URIStatus} in the result of {@link #listStatus}.
   * This method is preferred when iterating over directories with a large number of files or
   * sub-directories inside. The caller can proceed with partial result without waiting for all
   * result returned.
   *
   * @param path the path to list information about
   * @param options options to associate with this operation
   * @param action action to apply on each {@code URIStatus}
   * @throws FileDoesNotExistException if the given path does not exist
   */
  void iterateStatus(AlluxioURI path, ListStatusPOptions options,
      Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #listStatus(AlluxioURI, ListStatusPOptions)} with default
   * options.
   *
   * @param path the path to list information about
   * @return a list of {@link URIStatus}s containing information about the files and directories
   *         which are children of the given path
   * @throws FileDoesNotExistException if the given path does not exist
   */
  default List<URIStatus> listStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return listStatus(path, ListStatusPOptions.getDefaultInstance());
  }

  /**
   * If the path is a directory, returns the {@link URIStatus} of all the direct entries in it.
   * Otherwise returns a list with a single {@link URIStatus} element for the file.
   *
   * @param path the path to list information about
   * @param options options to associate with this operation
   * @return a list of {@link URIStatus}s containing information about the files and directories
   *         which are children of the given path
   * @throws FileDoesNotExistException if the given path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #loadMetadata(AlluxioURI, ListStatusPOptions)} with default
   * options.
   *
   * @param path the path for which to load metadata from UFS
   * @throws FileDoesNotExistException if the given path does not exist
   */
  default void loadMetadata(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.ALWAYS)
        .setRecursive(LoadMetadataPOptions.getDefaultInstance().getRecursive())
        .setLoadMetadataOnly(true).build();
    loadMetadata(path, options);
  }

  /**
   * Loads metadata about a path in the UFS to Alluxio. No data will be transferred.
   *
   * @param path the path for which to load metadata from UFS
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   */
  void loadMetadata(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #mount(AlluxioURI, AlluxioURI, MountPOptions)} with default
   * options.
   *
   * @param alluxioPath an Alluxio path to mount the data to
   * @param ufsPath a UFS path to mount the data from
   */
  default void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath)
      throws IOException, AlluxioException {
    mount(alluxioPath, ufsPath, MountPOptions.getDefaultInstance());
  }

  /**
   * Mounts a UFS subtree to the given Alluxio path. The Alluxio path is expected not to exist as
   * the method creates it. If the path already exists, a {@link AlluxioException} will be thrown.
   * This method does not transfer any data or metadata from the UFS. It simply establishes the
   * connection between the given Alluxio path and UFS path.
   *
   * @param alluxioPath an Alluxio path to mount the data to
   * @param ufsPath a UFS path to mount the data from
   * @param options options to associate with this operation
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws IOException, AlluxioException;

  /**
   * Updates the options for an existing mount point.
   *
   * @param alluxioPath the Alluxio path of the mount point
   * @param options options for this mount point
   */
  void updateMount(AlluxioURI alluxioPath, MountPOptions options)
      throws IOException, AlluxioException;

  /**
   * Lists all mount points and their corresponding under storage addresses.
   * @return a map from String to {@link MountPointInfo}
   */
  Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException;

  /**
   * Lists all the actively synced paths.
   *
   * @return a list of actively synced paths
   */
  List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException;

  /**
   * Convenience method for {@link #openFile(AlluxioURI, OpenFilePOptions)} with default options.
   *
   * @param path the file to read from
   * @return a {@link FileInStream} for the given path
   * @throws FileDoesNotExistException when path does not exist
   * @throws OpenDirectoryException when path is a directory
   * @throws FileIncompleteException when path is a file and is not completed yet
   */
  default FileInStream openFile(AlluxioURI path)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    return openFile(path, OpenFilePOptions.getDefaultInstance());
  }

  /**
   * Opens a file for reading.
   *
   * @param path the file to read from
   * @param options options to associate with this operation
   * @return a {@link FileInStream} for the given path
   * @throws FileDoesNotExistException when path does not exist
   * @throws OpenDirectoryException when path is a directory
   * @throws FileIncompleteException when path is a file and is not completed yet
   */
  FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException;

  /**
   * Opens a file for reading.
   *
   * @param status status of the file to read from
   * @param options options to associate with this operation
   * @return a {@link FileInStream} for the given path
   * @throws FileDoesNotExistException when path does not exist
   * @throws OpenDirectoryException when path is a directory
   * @throws FileIncompleteException when path is a file and is not completed yet
   */
  FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException;

  /**
   * Convenience method for {@link #persist(AlluxioURI, ScheduleAsyncPersistencePOptions)} which
   * uses the default {@link ScheduleAsyncPersistencePOptions}.
   *
   * @param path the uri of the file to persist
   */
  default void persist(final AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    persist(path, ScheduleAsyncPersistencePOptions.getDefaultInstance());
  }

  /**
   * Schedules the given path to be asynchronously persisted to the under file system.
   *
   * To persist synchronously please see
   * {@link FileSystemUtils#persistAndWait(FileSystem, AlluxioURI, long)}.
   *
   * @param path the uri of the file to persist
   * @param options the options to use when submitting persist the path
   */
  void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #rename(AlluxioURI, AlluxioURI, RenamePOptions)} with default
   * options.
   *
   * @param src the path of the source, this must already exist
   * @param dst the path of the destination, this path should not exist
   * @throws FileDoesNotExistException if the given file does not exist
   */
  default void rename(AlluxioURI src, AlluxioURI dst)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rename(src, dst, RenamePOptions.getDefaultInstance());
  }

  /**
   * Renames an existing Alluxio path to another Alluxio path in Alluxio. This operation will be
   * propagated in the underlying storage if the path is persisted.
   *
   * @param src the path of the source, this must already exist
   * @param dst the path of the destination, this path should not exist
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Reverse resolve a ufs uri.
   *
   * @param ufsUri the ufs uri
   * @return the alluxio path for the ufsUri
   * @throws AlluxioStatusException
   */
  AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException;

  /**
   * Convenience method for {@link #setAcl(AlluxioURI, SetAclAction, List, SetAclPOptions)} with
   * default options.
   *
   * @param path the path to set the ACL for
   * @param action the set action to perform
   * @param entries the ACL entries
   * @throws FileDoesNotExistException if the given file does not exist
   */
  default void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries)
      throws FileDoesNotExistException, IOException, AlluxioException {
    setAcl(path, action, entries, SetAclPOptions.getDefaultInstance());
  }

  /**
   * Sets the ACL for a path.
   *
   * @param path the path to set the ACL for
   * @param action the set action to perform
   * @param entries the ACL entries
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Starts the active syncing process on an Alluxio path.
   * @param path the path to sync
   */
  void startSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Stops the active syncing process on an Alluxio path.
   * @param path the path to stop syncing
   */
  void stopSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #setAttribute(AlluxioURI, SetAttributePOptions)} with default
   * options.
   *
   * @param path the path to set attributes for
   * @throws FileDoesNotExistException if the given file does not exist
   */
  default void setAttribute(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    setAttribute(path, SetAttributePOptions.getDefaultInstance());
  }

  /**
   * Sets any number of a path's attributes, such as TTL and pin status.
   *
   * @param path the path to set attributes for
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #unmount(AlluxioURI, UnmountPOptions)} with default options.
   *
   * @param path an Alluxio path, this must be a mount point
   */
  default void unmount(AlluxioURI path) throws IOException, AlluxioException {
    unmount(path, UnmountPOptions.getDefaultInstance());
  }

  /**
   * Unmounts a UFS subtree identified by the given Alluxio path. The Alluxio path match a
   * previously mounted path. The contents of the subtree rooted at this path are removed from
   * Alluxio but the corresponding UFS subtree is left untouched.
   *
   * @param path an Alluxio path, this must be a mount point
   * @param options options to associate with this operation
   */
  void unmount(AlluxioURI path, UnmountPOptions options) throws IOException, AlluxioException;
}
