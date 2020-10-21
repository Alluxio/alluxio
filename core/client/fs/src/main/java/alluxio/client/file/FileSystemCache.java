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
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.master.MasterInquireClient;
import alluxio.security.authorization.AclEntry;
import alluxio.uri.Authority;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A cache for storing {@link FileSystem} clients. This should only be used by the Factory class.
 */
@ThreadSafe
public class FileSystemCache {
  private final Object mLock = new Object();

  @GuardedBy("mLock")
  private final HashMap<Key, Value> mCacheMap = new HashMap<>();

  /**
   * Constructs a new cache for file system instances.
   */
  public FileSystemCache() { }

  /**
   * Gets a {@link FileSystem} instance from the cache. If there is none,
   * a new instance is created and inserted into the cache.
   * Note that, the returned instance will be a wrapper of the cached instance which
   * has its own close state.
   *
   * @param key the key to retrieve a {@link FileSystem}
   * @return the {@link FileSystem} associated with the key
   */
  public FileSystem get(Key key) {
    synchronized (mLock) {
      Value value = mCacheMap.get(key);
      FileSystem fs;
      if (value == null) {
        // On cache miss, create and insert a new FileSystem instance,
        fs = FileSystem.Factory.create(FileSystemContext.create(key.mSubject, key.mConf));
        mCacheMap.put(key, new Value(fs, 1));
      } else {
        fs = value.mFileSystem;
        value.mRefCount.getAndIncrement();
      }
      return new InstanceCachingFileSystem(fs, key);
    }
  }

  /**
   * Closes and removes all {@link FileSystem} from the cache. Only to be used for testing
   * purposes.
   */
  @VisibleForTesting
  void purge() {
    synchronized (mLock) {
      new HashSet<>(mCacheMap.values()).forEach(value -> {
        try {
          value.mRefCount.set(1);
          value.mFileSystem.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /**
   * A key which can be used to look up a {@link FileSystem} instance in the
   * {@link FileSystemCache}.
   */
  public static class Key {
    final Subject mSubject;
    final Authority mAuth;

    /**
     * Only used to store the configuration. Allows us to compute a {@link FileSystem} directly
     * from a key.
     */
    final AlluxioConfiguration mConf;

    /**
     * @param subject Subject of the user
     * @param conf Alluxio configuration
     */
    public Key(Subject subject, AlluxioConfiguration conf) {
      mConf = conf;
      mSubject = subject;
      mAuth = MasterInquireClient.Factory.getConnectDetails(conf).toAuthority();
    }

    @Override
    public int hashCode() {
      return Objects.hash(mSubject, mAuth);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Key)) {
        return false;
      }
      Key otherKey = (Key) o;
      return Objects.equals(mSubject, otherKey.mSubject)
          && Objects.equals(mAuth, otherKey.mAuth);
    }
  }

  /**
   * A value wraps a {@link FileSystem} instance and a ref count in the {@link FileSystemCache}.
   */
  public static class Value {
    final FileSystem mFileSystem;
    final AtomicInteger mRefCount;

    /**
     * @param fileSystem filesystem instance cached
     * @param count initial ref count
     */
    public Value(FileSystem fileSystem, int count) {
      mFileSystem = fileSystem;
      mRefCount = new AtomicInteger(count);
    }
  }

  /**
   * A wrapper class on a FileSystem instance. When closing an instance of this wrapper class, it
   * will first decrement the refcount of the underlying File System instance and only when this
   * ref count becomes zero, remove the instance from the cache and release this resource used.
   */
  public class InstanceCachingFileSystem extends DelegatingFileSystem {
    public static final String CLOSED_FS_ERROR_MESSAGE = "FileSystem already closed";

    final Key mKey;
    boolean mClosed = false;

    /**
     * Wraps a file system instance to cache.
     *
     * @param fs fs instance
     * @param key key in fs instance cache
     */
    InstanceCachingFileSystem(FileSystem fs, Key key) {
      super(fs);
      mKey = key;
    }

    @Override
    public boolean isClosed() {
      return mClosed;
    }

    @Override
    public void close() throws IOException {
      if (!mClosed) {
        mClosed = true;
        // Decrement the ref count. If the ref count goes zero, remove the entry from cache
        synchronized (FileSystemCache.this.mLock) {
          Value value = mCacheMap.get(mKey);
          Preconditions.checkNotNull(value);
          if (value.mRefCount.decrementAndGet() == 0) {
            mCacheMap.remove(mKey);
            super.close();
          }
        }
      }
    }

    @Override
    public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
        throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.createDirectory(path, options);
    }

    @Override
    public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
        throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.createFile(path, options);
    }

    @Override
    public void delete(AlluxioURI path, DeletePOptions options) throws DirectoryNotEmptyException,
        FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.delete(path, options);
    }

    @Override
    public boolean exists(AlluxioURI path, ExistsPOptions options)
        throws InvalidPathException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.exists(path, options);
    }

    @Override
    public void free(AlluxioURI path, FreePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.free(path, options);
    }

    @Override
    public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.getBlockLocations(path);
    }

    @Override
    public AlluxioConfiguration getConf() {
      return super.getConf();
    }

    @Override
    public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.getStatus(path, options);
    }

    @Override
    public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.listStatus(path, options);
    }

    @Override
    public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
                              Consumer<? super URIStatus> action)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.iterateStatus(path, options, action);
    }

    @Override
    public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
        throws IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.mount(alluxioPath, ufsPath, options);
    }

    @Override
    public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
        throws IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.updateMount(alluxioPath, options);
    }

    @Override
    public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.getMountTable();
    }

    @Override
    public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.getSyncPathList();
    }

    @Override
    public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
        throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
        IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.openFile(path, options);
    }

    @Override
    public FileInStream openFile(URIStatus status, OpenFilePOptions options)
        throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
        IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.openFile(status, options);
    }

    @Override
    public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.persist(path, options);
    }

    @Override
    public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.rename(src, dst, options);
    }

    @Override
    public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      return super.reverseResolve(ufsUri);
    }

    @Override
    public void setAcl(
        AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclPOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.setAcl(path, action, entries, options);
    }

    @Override
    public void startSync(AlluxioURI path)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.startSync(path);
    }

    @Override
    public void stopSync(AlluxioURI path)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.stopSync(path);
    }

    @Override
    public void setAttribute(AlluxioURI path, SetAttributePOptions options)
        throws FileDoesNotExistException, IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.setAttribute(path, options);
    }

    @Override
    public void unmount(AlluxioURI path, UnmountPOptions options)
        throws IOException, AlluxioException {
      if (mClosed) {
        throw new IOException(CLOSED_FS_ERROR_MESSAGE);
      }
      super.unmount(path, options);
    }
  }
}
