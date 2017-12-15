package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;

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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An under filesystem input stream manager that can cache seekable input streams for future reuse.
 * The manager uses guava's Cache to store the input streams, with a time-based eviction policy of
 * expiration configured in {@link PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRE_MS}.
 *
 * An under storage file maybe opened with multiple input streams at the same time. The manager uses
 * {@link UfsInputStreamIds} to track the in-use input stream and the available ones. The
 * manager closes the input streams after they are expired and not in-use anymore.
 */
@ThreadSafe
public class UfsInputStreamManager {
  private static final Logger LOG = LoggerFactory.getLogger(UfsInputStreamManager.class);

  /**
   * A map from the ufs file name to the metadata of the input streams. Synchronization is needed to
   * access this map.
   */
  private final HashMap<String, UfsInputStreamIds> mFileToInputStreamIds;
  /**
   * Cache of the input streams, from the input stream id to the input stream.
   */
  private final Cache<Long, SeekableUnderFileInputStream> mUnderFileInputStreamCache;
  /**
   * Thread pool for asynchronously removing the expired input streams.
   */
  private final ExecutorService mRemovalThreadPool;

  /**
   * A listener to the input stream removal.
   */
  private RemovalListener<Long, SeekableUnderFileInputStream> mRemovalListener =
      new RemovalListener<Long, SeekableUnderFileInputStream>() {
        @Override
        public void onRemoval(RemovalNotification<Long, SeekableUnderFileInputStream> removal) {
          SeekableUnderFileInputStream inputStream = removal.getValue();
          synchronized (mFileToInputStreamIds) {
            if (mFileToInputStreamIds.containsKey(inputStream.getFilePath())) {
              UfsInputStreamIds resources = mFileToInputStreamIds.get(inputStream.getFilePath());
              synchronized (resources) {
                // remove the key
                resources.removeInUse(removal.getKey());
                if (resources.removeAvailable(removal.getKey())) {
                  // close the resource
                  LOG.debug("Removed the under file input stream resource of {}", removal.getKey());
                  try {
                    inputStream.close();
                  } catch (IOException e) {
                    LOG.warn("Failed to close the input stream resource of file {} and resource id",
                        inputStream.getFilePath(), removal.getKey());
                  }
                }
                if (resources.isEmpty()) {
                  // remove the resources entry
                  mFileToInputStreamIds.remove(inputStream.getFilePath());
                }
              }
            } else {
              LOG.warn("Try to remove the resource entry of {} but not exists any more",
                  removal.getKey());
            }
          }
        }
      };

  /**
   * Creates a new {@link UfsInputStreamManager}.
   */
  public UfsInputStreamManager() {
    mFileToInputStreamIds = new HashMap<>();
    mRemovalThreadPool = Executors.newFixedThreadPool(2);

    mUnderFileInputStreamCache = CacheBuilder.newBuilder()
        .expireAfterAccess(Configuration.getMs(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRE_MS),
            TimeUnit.MILLISECONDS)
        .removalListener(RemovalListeners.asynchronous(mRemovalListener, mRemovalThreadPool))
        .build();
  }

  /**
   * Checks in an input stream. The input stream is closed if it's already expired.
   *
   * @param inputStream the input stream to check in
   * @throws IOException when input stream fails to close
   */
  public void checkIn(InputStream inputStream) throws IOException {
    // for non-seekable input stream, close and return
    if (!(inputStream instanceof SeekableUnderFileInputStream)) {
      inputStream.close();
      return;
    }

    SeekableUnderFileInputStream seekableInputStream = (SeekableUnderFileInputStream) inputStream;
    synchronized (mFileToInputStreamIds) {
      if (!mFileToInputStreamIds.containsKey(seekableInputStream.getFilePath())) {
        LOG.debug("The resource {} is already expired", seekableInputStream.getResourceId());
        // the cache no longer tracks this input stream
        seekableInputStream.close();
        return;
      }
      UfsInputStreamIds resources = mFileToInputStreamIds.get(seekableInputStream.getFilePath());
      if (!resources.checkIn(seekableInputStream.getResourceId())) {
        LOG.debug("Close the expired input stream resource of {}",
            seekableInputStream.getResourceId());
        // the input stream expired, close it
        seekableInputStream.close();
      }
    }
  }

  /**
   * Checks out an input stream. If there is available input stream in the cache, reuse it and
   * repositions the offset, otherwise the manager opens a new input stream.
   *
   * @param ufs the under file system
   * @param path the path to the under storage file
   * @param offset the offset to open
   * @return the checked out input stream
   * @throws IOException if the input stream fails to open
   */
  public InputStream checkOut(UnderFileSystem ufs, String path, long offset) throws IOException {
    if (!ufs.isSeekable()) {
      // not able to cache, always return a new input stream
      return ufs.open(path, OpenOptions.defaults().setOffset(offset));
    }

    UfsInputStreamIds resources;
    synchronized (mFileToInputStreamIds) {
      if (mFileToInputStreamIds.containsKey(path)) {
        resources = mFileToInputStreamIds.get(path);
      } else {
        resources = new UfsInputStreamIds();
        mFileToInputStreamIds.put(path, resources);
      }
    }

    synchronized (resources) {
      long nextId = -1;
      SeekableUnderFileInputStream inputStream = null;
      // find the next available input stream from the cache
      for (long id : resources.availableIds()) {
        inputStream = mUnderFileInputStreamCache.getIfPresent(id);
        if (inputStream != null) {
          nextId = id;
          LOG.debug("Reused the under file input stream resource of {}", nextId);
          // for the cached ufs instream, seek to the requested position
          inputStream.seek(offset);
          break;
        }
      }
      // no cached input stream is available, open a new one
      if (nextId == -1) {
        nextId = IdUtils.getRandomNonNegativeLong();
        final long newId = nextId;
        try {
          inputStream = mUnderFileInputStreamCache.get(nextId, () -> {
            SeekableUnderFileInputStream ufsStream = (SeekableUnderFileInputStream) ufs.open(path,
                OpenOptions.defaults().setOffset(offset));
            LOG.debug("Created the under file input stream resource of {}", newId);
            ufsStream.setResourceId(newId);
            ufsStream.setFilePath(path);
            return ufsStream;
          });
        } catch (ExecutionException e) {
          LOG.warn("Failed to retrieve the cached UFS instream");
          // fall back to a ufs creation.
          return ufs.open(path, OpenOptions.defaults().setOffset(offset));
        }
      }

      // mark the input stream id as checked out
      resources.checkOut(nextId);
      return inputStream;
    }
  }

  /**
   * The metadata of the input streams associated with an under storage file that tracks which input
   * streams are in-use or available. Each input stream is identified by a unique id.
   */
  @ThreadSafe
  private static class UfsInputStreamIds {
    private final Set<Long> mInUse;
    private final Set<Long> mAvailable;

    /**
     * Creates a new {@link UfsInputStreamIds}.
     */
    UfsInputStreamIds() {
      mInUse = new HashSet<>();
      mAvailable = new HashSet<>();
    }

    /**
     * @return the a view of the available input stream' id.
     */
    Set<Long> availableIds() {
      return Collections.unmodifiableSet(mAvailable);
    }

    /**
     * Marks an input stream as checked out.
     *
     * @param id the id of the input stream
     */
    public synchronized void checkOut(long id) {
      Preconditions.checkArgument(!mInUse.contains(id), id + " is already in use");
      mAvailable.remove(id);
      mInUse.add(id);
    }

    /**
     * @return if there is any outstanding input streams of the file
     */
    public synchronized boolean isEmpty() {
      return mInUse.isEmpty() && mAvailable.isEmpty();
    }

    /**
     * Marks an input stream as not in use.
     * @param id the id of the input stream
     */
    public synchronized void removeInUse(long id) {
      mInUse.remove(id);
    }

    /**
     * Removes the mark of the input stream as available.
     *
     * @param id the id of the input stream
     * @return <code>true</code> if the given input stream is available, <code>false</code> if the
     *         given input stream is not among available input streams
     */
    public synchronized boolean removeAvailable(long id) {
      return mAvailable.remove(id);
    }

    /**
     * Returns an id to the input stream pool. If marks the id from in use to available. If the
     * input stream is already removed from the cache, then do nothing.
     *
     * @param id id of the input stream
     * @return true if the id is marked from in use to available; false if the id no longer used for
     *         cache
     */
    public synchronized boolean checkIn(long id) {
      Preconditions.checkArgument(!mAvailable.contains(id));
      if (mInUse.contains(id)) {
        mInUse.remove(id);
        mAvailable.add(id);
        return true;
      }
      return false;
    }
  }
}
