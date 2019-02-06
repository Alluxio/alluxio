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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.master.MasterInquireClient;
import alluxio.security.authorization.AclEntry;
import alluxio.uri.Authority;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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

    protected static final FileSystem.Cache FILESYSTEM_CACHE = new FileSystem.Cache();

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
      Preconditions.checkNotNull(subject, "subject");
      AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
      FileSystemKey key = new FileSystemKey(subject, conf);
      return FILESYSTEM_CACHE.get(key);
    }

    /**
     * @param alluxioConf the configuration to utilize with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystem create(AlluxioConfiguration alluxioConf) {
      return create(FileSystemContext.create(alluxioConf), false);
    }

    /**
     * @param ctx the context with the subject and configuration to utilize with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystem create(ClientContext ctx) {
      return create(FileSystemContext.create(ctx), false);
    }

    /**
     * @param context the FileSystemContext to use with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystem create(FileSystemContext context) {
      return create(context, false);
    }

    private static FileSystem create(FileSystemContext context, boolean cachingEnabled) {
      if (LOG.isDebugEnabled() && !CONF_LOGGED.getAndSet(true)) {
        // Sort properties by name to keep output ordered.
        AlluxioConfiguration conf = context.getConf();
        List<PropertyKey> keys = new ArrayList<>(conf.keySet());
        Collections.sort(keys, Comparator.comparing(PropertyKey::getName));
        for (PropertyKey key : keys) {
          String value = conf.getOrDefault(key, null);
          Source source = conf.getSource(key);
          LOG.debug("{}={} ({})", key.getName(), value, source);
        }
      }
      return BaseFileSystem.create(context, cachingEnabled);
    }
  }

  /**
   * A cache for storing {@link FileSystem} clients. This should only be used by the Factory class.
   */
  class Cache {
    final ConcurrentHashMap<FileSystemKey, FileSystem> mCacheMap = new ConcurrentHashMap<>();

    public Cache() { }

    /**
     * Gets a {@link FileSystem} from the cache. If there is none, one is created, inserted into
     * the cache, and returned back to the user.
     *
     * @param key the key to retrieve a {@link FileSystem}
     * @return the {@link FileSystem} associated with the key
     */
    public FileSystem get(FileSystemKey key) {
      return mCacheMap.computeIfAbsent(key, (fileSystemKey) ->
          Factory.create(FileSystemContext.create(key.mSubject, key.mConf), true));
    }

    /**
     * Removes the client with the given key from the cache. Returns the client back to the user.
     *
     * @param key the client key to remove
     * @return The removed context or null if there is no client associated with the key
     */
    public FileSystem remove(FileSystemKey key) {
      return mCacheMap.remove(key);
    }

    /**
     * Closes and removes all {@link FileSystem} from the cache. Only to be used for testing
     * purposes. This method operates on the assumption that no concurrent calls to get/remove
     * will be made while this function is running.
     */
    @VisibleForTesting
    void purge() {
      mCacheMap.forEach((fsKey, fs) -> {
        try {
          mCacheMap.remove(fsKey);
          fs.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * A key which can be used to look up a {@link FileSystem} instance in the {@link Cache}.
   */
  class FileSystemKey {
    final Subject mSubject;
    final Authority mAuth;

    /**
     * Only used to store the configuration. Allows us to compute a {@link FileSystem} directly
     * from a key.
     */
    final AlluxioConfiguration mConf;

    public FileSystemKey(Subject subject, AlluxioConfiguration conf) {
      mConf = conf;
      mSubject = subject;
      mAuth = MasterInquireClient.Factory.getConnectDetails(conf).toAuthority();
    }

    public FileSystemKey(ClientContext ctx) {
      this(ctx.getSubject(), ctx.getConf());
    }

    @Override
    public int hashCode() {
      return Objects.hash(mSubject, mAuth);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FileSystemKey)) {
        return false;
      }
      FileSystemKey otherKey = (FileSystemKey) o;
      return Objects.equals(mSubject, otherKey.mSubject)
          && Objects.equals(mAuth, otherKey.mAuth);
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
   * Convenience method for {@link #createDirectory(AlluxioURI, CreateDirectoryPOptions)} with
   * default options.
   *
   * @param path the path of the directory to create in Alluxio space
   * @throws FileAlreadyExistsException if there is already a file or directory at the given path
   * @throws InvalidPathException if the path is invalid
   */
  void createDirectory(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

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
  FileOutStream createFile(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

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
  void delete(AlluxioURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException;

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
  boolean exists(AlluxioURI path) throws InvalidPathException, IOException, AlluxioException;

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
  void free(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException;

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
   * Builds a mapping of {@link FileBlockInfo} to a list of {@link WorkerNetAddress} which allows a
   * user to determine the physical location of a file stored within Alluxio. In the case where
   * data is stored in a UFS, but not in Alluxio this function will only include a
   * {@link WorkerNetAddress} if the block stored in the UFS is co-located with an Alluxio worker.
   * However if there are no co-located Alluxio workers for the block, then the behavior is
   * controlled by the {@link PropertyKey#USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED} . If
   * this property is set to {@code true} then every Alluxio worker will be returned.
   * Blocks which are stored in the UFS and are *not* co-located with any Alluxio worker will return
   * an empty list. If the file block is within Alluxio *and* the UFS then this will only return
   * Alluxio workers which currently store the block.
   *
   * @param path the path to get block info for
   * @return a map of blocks to the workers whose hosts have the blocks. The blocks may not
   *         necessarily be stored in Alluxio
   * @throws FileDoesNotExistException if the given path does not exist
   */
  Map<FileBlockInfo, List<WorkerNetAddress>> getBlockLocations(AlluxioURI path)
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
  URIStatus getStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

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
   * Convenience method for {@link #listStatus(AlluxioURI, ListStatusPOptions)} with default
   * options.
   *
   * @param path the path to list information about
   * @return a list of {@link URIStatus}s containing information about the files and directories
   *         which are children of the given path
   * @throws FileDoesNotExistException if the given path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

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
   * Convenience method for {@link #loadMetadata(AlluxioURI, LoadMetadataPOptions)} with default
   * options.
   *
   * @param path the path for which to load metadata from UFS
   * @throws FileDoesNotExistException if the given path does not exist
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  void loadMetadata(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Loads metadata about a path in the UFS to Alluxio. No data will be transferred.
   *
   * @param path the path for which to load metadata from UFS
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  void loadMetadata(AlluxioURI path, LoadMetadataPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #mount(AlluxioURI, AlluxioURI, MountPOptions)} with default
   * options.
   *
   * @param alluxioPath an Alluxio path to mount the data to
   * @param ufsPath a UFS path to mount the data from
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath) throws IOException, AlluxioException;

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
   * @throws FileDoesNotExistException if the given file does not exist
   */
  FileInStream openFile(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Opens a file for reading.
   *
   * @param path the file to read from
   * @param options options to associate with this operation
   * @return a {@link FileInStream} for the given path
   * @throws FileDoesNotExistException if the given file does not exist
   */
  FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #rename(AlluxioURI, AlluxioURI, RenamePOptions)} with default
   * options.
   *
   * @param src the path of the source, this must already exist
   * @param dst the path of the destination, this path should not exist
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst)
      throws FileDoesNotExistException, IOException, AlluxioException;

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
   * Convenience method for {@link #setAcl(AlluxioURI, SetAclAction, List, SetAclPOptions)} with
   * default options.
   *
   * @param path the path to set the ACL for
   * @param action the set action to perform
   * @param entries the ACL entries
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries)
      throws FileDoesNotExistException, IOException, AlluxioException;

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
  void setAttribute(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

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
  void unmount(AlluxioURI path) throws IOException, AlluxioException;

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
