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

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.logging.SamplingLogger;
import alluxio.worker.block.io.BlockReader;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class implements a {@link BlockReader} to read a block directly from UFS, and
 * optionally cache the block to the Alluxio worker if the whole block it is read.
 */
@ThreadSafe
public final class UfsInputStreamCache {
  private static final Logger LOG = LoggerFactory.getLogger(UfsInputStreamCache.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.MINUTE_MS);
  private static final boolean CACHE_ENABLED =
      ServerConfiguration.getBoolean(PropertyKey.WORKER_UFS_INSTREAM_CACHE_ENABLED);

  /**
   * A map from the ufs file id to the metadata of the input streams. Synchronization on this map
   * before access.
   */
  private final Map<Long, StreamIdSet> mFileIdToStreamIds;
  /** Cache of the input streams, from the input stream id to the input stream. */
  private final Cache<Long, CachedSeekableInputStream> mStreamCache;
  /** Thread pool for asynchronously removing the expired input streams. */
  private final ExecutorService mRemovalThreadPool;

  /**
   * Constructs a new UFS input stream cache.
   */
  public UfsInputStreamCache() {
    mFileIdToStreamIds = new ConcurrentHashMap<>();
    mRemovalThreadPool = ExecutorServiceFactories
        .fixedThreadPool(Constants.UFS_INPUT_STREAM_CACHE_EXPIRATION, 2)
        .create();

    // A listener to the input stream removal.
    RemovalListener<Long, CachedSeekableInputStream> listener =
        (RemovalNotification<Long, CachedSeekableInputStream> removal) -> {
          CachedSeekableInputStream inputStream = removal.getValue();
          final long fileId = inputStream.getFileId();
          final long resourceId = removal.getKey();
          boolean shouldClose = false;

          StreamIdSet streamIds = mFileIdToStreamIds.get(fileId);
          if (streamIds == null) {
            LOG.warn(
                "Removed UFS input stream (fileId: {} resourceId: {}) but does not exist",
                fileId, resourceId);
          } else {
            synchronized (streamIds) {
              // remove the key
              if (streamIds.removeInUse(resourceId)) {
                LOG.warn("Removed in-use UFS input stream (fileId: {} resourceId: {})", fileId,
                    resourceId);
              }
              if (streamIds.removeAvailable(resourceId)) {
                // close the resource
                LOG.debug("Removed available UFS input stream (fileId: {} resourceId: {})", fileId,
                    resourceId);
                shouldClose = true;
              }
              if (streamIds.isEmpty()) {
                // remove the value from the mapping
                mFileIdToStreamIds.remove(fileId);
              }
            }
          }

          if (shouldClose) {
            try {
              inputStream.close();
            } catch (IOException e) {
              LOG.warn("Failed to close UFS input stream resource of file {} with file id {}"
                      + " and resource id {}", inputStream.getFilePath(), inputStream.getFileId(),
                  resourceId);
            }
          }
        };
    mStreamCache = CacheBuilder.newBuilder()
        .maximumSize(ServerConfiguration.getInt(PropertyKey.WORKER_UFS_INSTREAM_CACHE_MAX_SIZE))
        .expireAfterAccess(
            ServerConfiguration.getMs(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRARTION_TIME),
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
    if (!(inputStream instanceof CachedSeekableInputStream) || !CACHE_ENABLED) {
      // for non-seekable input stream, close and return
      inputStream.close();
      return;
    }

    CachedSeekableInputStream cachedStream = (CachedSeekableInputStream) inputStream;
    long fileId = cachedStream.getFileId();
    long resourceId = cachedStream.getResourceId();
    StreamIdSet streamIds = mFileIdToStreamIds.get(fileId);

    if (streamIds == null) {
      // the cache no longer tracks this input stream
      LOG.debug("UFS input stream (fileId: {} resourceId: {}) is already expired", fileId,
          resourceId);
      inputStream.close();
      return;
    }

    if (!streamIds.release(resourceId)) {
      LOG.debug("Close the expired UFS input stream (fileId: {} resourceId: {})", fileId,
          resourceId);
      // the input stream expired, close it
      inputStream.close();
    }
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
    if (!ufs.isSeekable() || !CACHE_ENABLED) {
      // only seekable UFSes are cachable/reusable, always return a new input stream
      return ufs.openExistingFile(path, openOptions);
    }

    // explicit cache cleanup
    try {
      mStreamCache.cleanUp();
    } catch (Throwable error) {
      SAMPLING_LOG.warn("Explicit cache removal failed.", error);
    }

    StreamIdSet streamIds = mFileIdToStreamIds.compute(fileId, (key, value) -> {
      if (value != null) {
        return value;
      }
      return new StreamIdSet();
    });

    // Try to acquire an existing id from the stream id set.
    // synchronized is required to be consistent between availableIds() and acquire(id).
    CachedSeekableInputStream inputStream = null;
    synchronized (streamIds) {
      // find the next available input stream from the cache
      for (long id : streamIds.availableIds()) {
        inputStream = mStreamCache.getIfPresent(id);
        if (inputStream != null) {
          // acquire it now while locked, so other threads cannot take it
          streamIds.acquire(id);
          break;
        }
      }
    }

    if (inputStream != null) {
      // for the cached ufs instream, seek (outside of critical section) to the requested position.
      LOG.debug("Reused the under file input stream resource of {}", inputStream.getResourceId());
      inputStream.seek(openOptions.getOffset());
      return inputStream;
    }

    // no cached input stream is available, acquire a new id and open a new stream
    final long newId = streamIds.acquireNewId();
    try {
      inputStream = mStreamCache.get(newId, () -> {
        SeekableUnderFileInputStream ufsStream = (SeekableUnderFileInputStream) ufs
            .openExistingFile(path,
                OpenOptions.defaults().setPositionShort(openOptions.getPositionShort())
                    .setOffset(openOptions.getOffset()));
        LOG.debug("Created the under file input stream resource of {}", newId);
        return new CachedSeekableInputStream(ufsStream, newId, fileId, path);
      });
    } catch (ExecutionException e) {
      LOG.warn("Failed to create a new cached ufs instream of file id {} and path {}", fileId,
          path, e);
      // fall back to an uncached ufs creation.
      return ufs.openExistingFile(path,
          OpenOptions.defaults().setOffset(openOptions.getOffset()));
    }
    return inputStream;
  }

  /**
   * The metadata of the input streams associated with an under storage file that tracks which input
   * streams are in-use or available. Each input stream is identified by a unique id.
   */
  @ThreadSafe
  private static class StreamIdSet {
    private final Set<Long> mInUseStreamIds;
    private final Set<Long> mAvailableStreamIds;

    /**
     * Creates a new {@link StreamIdSet}.
     */
    StreamIdSet() {
      mInUseStreamIds = new HashSet<>();
      mAvailableStreamIds = new HashSet<>();
    }

    /**
     * @return a view of the available input stream ids
     */
    synchronized Set<Long> availableIds() {
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

    synchronized long acquireNewId() {
      while (true) {
        long newId = IdUtils.getRandomNonNegativeLong();
        if (mAvailableStreamIds.contains(newId) || mInUseStreamIds.contains(newId)) {
          // This id already managed, try again.
          continue;
        }

        acquire(newId);
        return newId;
      }
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
     * @return true if removed from the in-use streams
     */
    synchronized boolean removeInUse(long id) {
      return mInUseStreamIds.remove(id);
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
