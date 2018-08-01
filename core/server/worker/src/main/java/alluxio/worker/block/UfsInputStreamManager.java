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

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An under filesystem input stream manager that can cache seekable input streams for future reuse.
 * The manager caches the input streams, with a time-based eviction policy of expiration configured
 * in {@link PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRE_MS}.
 *
 * An under storage file maybe opened with multiple input streams at the same time. The manager uses
 * {@link UfsInputStreamIdSet} to track the in-use input stream and the available ones. The manager
 * closes the input streams after they are expired and not in-use anymore. Lock
 * {@link UfsInputStreamIdSet} before access, and in addition to that, lock
 * {@link #mFileIdToInputStreamIds} before the resource retrieval.
 */
@ThreadSafe
public class UfsInputStreamManager {
  private static final Logger LOG = LoggerFactory.getLogger(UfsInputStreamManager.class);
  private static final long UNAVAILABLE_RESOURCE_ID = -1;
  private static final boolean CACHE_ENABLED =
      Configuration.getBoolean(PropertyKey.WORKER_UFS_INSTREAM_CACHE_ENABLED);

  /**
   * A map from the ufs file id to the metadata of the input streams. Synchronization on this map
   * before access.
   */
  private final Map<Long, UfsInputStreamIdSet> mFileIdToInputStreamIds;
  /** Cache of the input streams, from the input stream id to the input stream. */
  @GuardedBy("mFileToInputStreamIds")
  private final Cache<Long, CachedSeekableInputStream> mUnderFileInputStreamCache;
  /** Thread pool for asynchronously removing the expired input streams. */
  private final ExecutorService mRemovalThreadPool;

  /**
   * Creates a new {@link UfsInputStreamManager}.
   */
  public UfsInputStreamManager() {
    mFileIdToInputStreamIds = new HashMap<>();
    mRemovalThreadPool = ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.UFS_INPUT_STREAM_CACHE_EXPIRATION, 2)
        .create();

    // A listener to the input stream removal.
    RemovalListener<Long, CachedSeekableInputStream> listener =
        (RemovalNotification<Long, CachedSeekableInputStream> removal) -> {
          CachedSeekableInputStream inputStream = removal.getValue();
          boolean shouldClose = false;
          synchronized (mFileIdToInputStreamIds) {
            if (mFileIdToInputStreamIds.containsKey(inputStream.getFileId())) {
              UfsInputStreamIdSet resources = mFileIdToInputStreamIds.get(inputStream.getFileId());
              synchronized (resources) {
                // remove the key
                resources.removeInUse(removal.getKey());
                if (resources.removeAvailable(removal.getKey())) {
                  // close the resource
                  LOG.debug("Removed the under file input stream resource of {}", removal.getKey());
                  shouldClose = true;
                }
                if (resources.isEmpty()) {
                  // remove the resources entry
                  mFileIdToInputStreamIds.remove(inputStream.getFileId());
                }
              }
            } else {
              LOG.warn("Try to remove the resource entry of {} but not exists any more",
                  removal.getKey());
            }
          }
          if (shouldClose) {
            try {
              inputStream.close();
            } catch (IOException e) {
              LOG.warn("Failed to close the input stream resource of file {} with file id {}",
                  " and resource id {}", inputStream.getFilePath(), inputStream.getFileId(),
                  removal.getKey());
            }
          }
        };
    mUnderFileInputStreamCache = CacheBuilder.newBuilder()
        .maximumSize(Configuration.getInt(PropertyKey.WORKER_UFS_INSTREAM_CACHE_MAX_SIZE))
        .expireAfterAccess(
            Configuration.getMs(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME),
            TimeUnit.MILLISECONDS)
        .removalListener(RemovalListeners.asynchronous(listener, mRemovalThreadPool)).build();
  }

  /**
   * Releases an input stream. The input stream is closed if it's already expired.
   *
   * @param inputStream the input stream to release
   * @throws IOException when input stream fails to close
   */
  public void release(InputStream inputStream) throws IOException {
    // for non-seekable input stream, close and return
    if (!(inputStream instanceof CachedSeekableInputStream) || !CACHE_ENABLED) {
      inputStream.close();
      return;
    }

    synchronized (mFileIdToInputStreamIds) {
      if (!mFileIdToInputStreamIds
          .containsKey(((CachedSeekableInputStream) inputStream).getFileId())) {
        LOG.debug("The resource {} is already expired",
            ((CachedSeekableInputStream) inputStream).getResourceId());
        // the cache no longer tracks this input stream
        inputStream.close();
        return;
      }
      UfsInputStreamIdSet resources =
          mFileIdToInputStreamIds.get(((CachedSeekableInputStream) inputStream).getFileId());
      if (!resources.release(((CachedSeekableInputStream) inputStream).getResourceId())) {
        LOG.debug("Close the expired input stream resource of {}",
            ((CachedSeekableInputStream) inputStream).getResourceId());
        // the input stream expired, close it
        inputStream.close();
      }
    }
  }

  /**
   * Invalidates an input stream from the cache.
   *
   * @param inputStream the cached input stream
   * @throws IOException when the invalidated input stream fails to release
   */
  public void invalidate(CachedSeekableInputStream inputStream) throws IOException {
    mUnderFileInputStreamCache.invalidate(inputStream.getResourceId());
    release(inputStream);
  }

  /**
   * Acquires an input stream. For seekable input streams, if there is an available input stream in
   * the cache, reuse it and repositions the offset, otherwise the manager opens a new input stream.
   *
   * @param ufs the under file system
   * @param path the path to the under storage file
   * @param fileId the file id
   * @param openOptions the open options
   * @return the acquired input stream
   * @throws IOException if the input stream fails to open
   */
  public InputStream acquire(UnderFileSystem ufs, String path, long fileId, OpenOptions openOptions)
      throws IOException {
    return acquire(ufs, path, fileId, openOptions, true);
  }

  /**
   * Acquires an input stream. For seekable input streams, if there is an available input stream in
   * the cache and reuse mode is specified, reuse it and repositions the offset, otherwise the
   * manager opens a new input stream.
   *
   * @param ufs the under file system
   * @param path the path to the under storage file
   * @param fileId the file id
   * @param openOptions the open options
   * @param reuse true to reuse existing input stream, otherwise acquire a new stream
   * @return the acquired input stream
   * @throws IOException if the input stream fails to open
   */
  public InputStream acquire(UnderFileSystem ufs, String path, long fileId, OpenOptions openOptions,
      boolean reuse) throws IOException {
    if (!ufs.isSeekable() || !CACHE_ENABLED) {
      // not able to cache, always return a new input stream
      return ufs.open(path, openOptions);
    }

    // explicit cache cleanup
    mUnderFileInputStreamCache.cleanUp();

    UfsInputStreamIdSet resources;
    synchronized (mFileIdToInputStreamIds) {
      if (mFileIdToInputStreamIds.containsKey(fileId)) {
        resources = mFileIdToInputStreamIds.get(fileId);
      } else {
        resources = new UfsInputStreamIdSet();
        mFileIdToInputStreamIds.put(fileId, resources);
      }
    }

    synchronized (resources) {
      long nextId = UNAVAILABLE_RESOURCE_ID;
      CachedSeekableInputStream inputStream = null;
      if (reuse) {
        // find the next available input stream from the cache
        for (long id : resources.availableIds()) {
          inputStream = mUnderFileInputStreamCache.getIfPresent(id);
          if (inputStream != null) {
            nextId = id;
            LOG.debug("Reused the under file input stream resource of {}", nextId);
            // for the cached ufs instream, seek to the requested position
            inputStream.seek(openOptions.getOffset());
            break;
          }
        }
      }
      // no cached input stream is available, open a new one
      if (nextId == UNAVAILABLE_RESOURCE_ID) {
        nextId = IdUtils.getRandomNonNegativeLong();
        final long newId = nextId;
        try {
          inputStream = mUnderFileInputStreamCache.get(nextId, () -> {
            SeekableUnderFileInputStream ufsStream = (SeekableUnderFileInputStream) ufs.open(path,
                OpenOptions.defaults().setOffset(openOptions.getOffset()));
            LOG.debug("Created the under file input stream resource of {}", newId);
            return new CachedSeekableInputStream(ufsStream, newId, fileId, path);
          });
        } catch (ExecutionException e) {
          LOG.warn("Failed to create a new cached ufs instream of file id {} and path {}", fileId,
              path);
          // fall back to a ufs creation.
          return ufs.open(path, OpenOptions.defaults().setOffset(openOptions.getOffset()));
        }
      }

      // mark the input stream id as acquired
      resources.acquire(nextId);
      return inputStream;
    }
  }

  /**
   * The metadata of the input streams associated with an under storage file that tracks which input
   * streams are in-use or available. Each input stream is identified by a unique id.
   */
  @ThreadSafe
  private static class UfsInputStreamIdSet {
    private final Set<Long> mInUseStreamIds;
    private final Set<Long> mAvailableStreamIds;

    /**
     * Creates a new {@link UfsInputStreamIdSet}.
     */
    UfsInputStreamIdSet() {
      mInUseStreamIds = new HashSet<>();
      mAvailableStreamIds = new HashSet<>();
    }

    /**
     * @return a view of the available input stream ids
     */
    Set<Long> availableIds() {
      return Collections.unmodifiableSet(mAvailableStreamIds);
    }

    /**
     * Marks an input stream as acquired.
     *
     * @param id the id of the input stream
     */
    synchronized void acquire(long id) {
      Preconditions.checkArgument(!mInUseStreamIds.contains(id), "%d is already in use", id);
      mAvailableStreamIds.remove(id);
      mInUseStreamIds.add(id);
    }

    /**
     * @return if there is any outstanding input streams of the file
     */
    synchronized boolean isEmpty() {
      return mInUseStreamIds.isEmpty() && mAvailableStreamIds.isEmpty();
    }

    /**
     * Marks an input stream as not in use.
     * @param id the id of the input stream
     */
    synchronized void removeInUse(long id) {
      mInUseStreamIds.remove(id);
    }

    /**
     * Removes the mark of the input stream as available.
     *
     * @param id the id of the input stream
     * @return <code>true</code> if the given input stream is available, <code>false</code> if the
     *         given input stream is not among available input streams
     */
    synchronized boolean removeAvailable(long id) {
      return mAvailableStreamIds.remove(id);
    }

    /**
     * Returns an id to the input stream pool. It marks the id from in use to available. If the
     * input stream is already removed from the cache, then do nothing.
     *
     * @param id id of the input stream
     * @return true if the id is marked from in use to available; false if the id no longer used for
     *         cache
     */
    synchronized boolean release(long id) {
      Preconditions.checkArgument(!mAvailableStreamIds.contains(id));
      if (mInUseStreamIds.contains(id)) {
        mInUseStreamIds.remove(id);
        mAvailableStreamIds.add(id);
        return true;
      }
      return false;
    }
  }
}
