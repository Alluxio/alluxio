package alluxio.worker.block;

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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class UnderFileInputStreamsManager {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileInputStreamsManager.class);

  public static final UnderFileInputStreamsManager INSTANCE = create();

  private final ConcurrentHashMap<String, UnderFileInputStreamResources> mResources;
  private final Cache<Long, SeekableUnderFileInputStream> mUnderFileInputStreamCache;
  private final ExecutorService mRemovalThreadPool;
  private final Future<?> mCacheCleanupService;

  private RemovalListener<Long, SeekableUnderFileInputStream> mRemovalListener =
      new RemovalListener<Long, SeekableUnderFileInputStream>() {
        @Override
        public void onRemoval(RemovalNotification<Long, SeekableUnderFileInputStream> removal) {
          SeekableUnderFileInputStream inputStream = removal.getValue();
          synchronized (mResources) {
            if (mResources.containsKey(inputStream.getFilePath())) {
              UnderFileInputStreamResources resources = mResources.get(inputStream.getFilePath());
              synchronized (resources) {
                // remove the key
                resources.removeInUse(removal.getKey());
                if (resources.removeAvailable(removal.getKey())) {
                  // close the resource
                  LOG.info("Removed the under file input stream resource of {}", removal.getKey());
                  try {
                    inputStream.close();
                  } catch (IOException e) {
                    LOG.warn("Failed to close the input stream resource of file {} and resource id",
                        inputStream.getFilePath(), removal.getKey());
                  }
                }
                if (resources.isEmpty()) {
                  // remove the resources entry
                  mResources.remove(inputStream.getFilePath());
                  LOG.info("Remove the resource entry of {}", inputStream.getFilePath());
                }
              }
            } else{
              LOG.info("Try to remove the resource entry of {} but not exists any more", removal.getKey());
            }
          }
        }
      };

  private UnderFileInputStreamsManager() {
    mResources=new ConcurrentHashMap<>();
    mRemovalThreadPool = Executors.newFixedThreadPool(2);

    mUnderFileInputStreamCache = CacheBuilder.newBuilder()
//        .expireAfterAccess(Configuration.getMs(PropertyKey.WORKER_UFS_INSTREAM_CACHE_EXPIRE_MS),
//            TimeUnit.MILLISECONDS)
        .expireAfterAccess(4000,
            TimeUnit.MILLISECONDS)
        .removalListener(RemovalListeners.asynchronous(mRemovalListener,mRemovalThreadPool)).build();
  }

  public static UnderFileInputStreamsManager create() {
    return new UnderFileInputStreamsManager();
  }

  public void checkIn(InputStream inputStream) throws IOException {
    if (!(inputStream instanceof SeekableUnderFileInputStream)) {
      inputStream.close();
      return;
    }

    SeekableUnderFileInputStream seekableInputStream = (SeekableUnderFileInputStream) inputStream;
    synchronized (mResources) {
      if (!mResources.containsKey(seekableInputStream.getFilePath())) {
        // the cache no longer tracks this input stream
        seekableInputStream.close();
        return;
      }
      UnderFileInputStreamResources resources = mResources.get(seekableInputStream.getFilePath());
      if (!resources.checkIn(seekableInputStream.getResourceId())) {
        LOG.info("Close the expired input stream resource of {}", seekableInputStream.getResourceId());
        seekableInputStream.close();
      }
    }
  }

  public InputStream checkOut(UnderFileSystem ufs, String path, long offset) throws IOException {
    if (!ufs.isSeekable()) {
      // not able to cache, always return a new input stream
      return ufs.open(path, OpenOptions.defaults().setOffset(offset));
    }

    UnderFileInputStreamResources resources;
    synchronized (mResources) {
      if(mResources.containsKey(path)) {
        resources=mResources.get(path);
      } else {
        resources = new UnderFileInputStreamResources();
        mResources.put(path, resources);
      }
    }

    synchronized (resources) {
      final long nextId = resources.nextAvailable();

      final long cacheId = nextId < 0 ? IdUtils.getRandomNonNegativeLong() : nextId;
      SeekableUnderFileInputStream inputStream=null;
      try {
        inputStream = mUnderFileInputStreamCache.get(cacheId, () -> {
          SeekableUnderFileInputStream ufsStream =
              (SeekableUnderFileInputStream) ufs.open(path, OpenOptions.defaults().setOffset(offset));
          LOG.info("Created the under file input stream resource of {}", cacheId);
          ufsStream.setResourceId(cacheId);
          ufsStream.setFilePath(path);
          return ufsStream;
        });
      } catch (ExecutionException e) {
        LOG.warn("Failed to retrieve the cached UFS instream");
        // fall back to a ufs creation.
        return ufs.open(path, OpenOptions.defaults().setOffset(offset));
      }
      resources.checkOut(cacheId);
      if (nextId>=0) {
        LOG.info("Reused the under file input stream resource of {}", nextId);
        // for the cached ufs instream, seek to the requested position
        inputStream.seek(offset);
      }

      return inputStream;
    }
  }

  @ThreadSafe
  static class UnderFileInputStreamResources {
    private final Set<Long> mInUse;
    private final Set<Long> mAvailable;

    UnderFileInputStreamResources() {
      mInUse = new HashSet<>();
      mAvailable= new HashSet<>();
    }

    public synchronized long nextAvailable() {
      if (mAvailable.isEmpty()){
        return -1;
      }
      return mAvailable.iterator().next();
    }

    public synchronized void checkOut(long id) {
      Preconditions.checkArgument(!mInUse.contains(id), id + " is already in use");
      mAvailable.remove(id);
      mInUse.add(id);
    }

    public synchronized boolean isEmpty() {
      return mInUse.isEmpty() || mAvailable.isEmpty();
    }

    public synchronized void removeInUse(long id) {
      mInUse.remove(id);
    }

    public synchronized boolean removeAvailable(long id) {
      return mAvailable.remove(id);
    }

    /**
     * Returns an id to the resource pool. If marks the id from in use to available. If the resource
     * is already removed from the cache, then do nothing.
     *
     * @param id id of the resource
     * @return true if the id is marked from in use to available; false if the id no longer used for
     *         cache
     */
    public synchronized boolean checkIn(long id) {
      Preconditions.checkArgument(!mAvailable.contains(id));
      if(mInUse.contains(id)) {
        mInUse.remove(id);
        mAvailable.add(id);
        return true;
      }
      return false;
    }
  }
}
