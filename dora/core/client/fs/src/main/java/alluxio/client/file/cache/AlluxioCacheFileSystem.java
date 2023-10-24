package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.CloseableSupplier;
import alluxio.PositionReader;
import alluxio.client.file.CacheContext;
import alluxio.client.file.DelegatingFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.MetadataCache;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.filter.CacheFilter;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.ThreadUtils;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Alluxio CacheFileSystem.
 */
public class AlluxioCacheFileSystem extends DelegatingFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCacheFileSystem.class);
  private static final int THREAD_KEEPALIVE_SECOND = 60;
  private static final int THREAD_TERMINATION_TIMEOUT_MS = 10000;
  private static final URIStatus NOT_FOUND_STATUS = new URIStatus(
      new FileInfo().setCompleted(true));

  private final Optional<CacheManager> mCacheManager;
  private final Optional<CacheFilter> mCacheFilter;
  private final AlluxioConfiguration mConf;
  private final Optional<MetadataCache> mMetadataCache;
  private final ExecutorService mAccessTimeUpdater;
  private final boolean mDisableUpdateFileAccessTime;

  /**
   * @param fs a FileSystem instance to query on local cache miss
   * @param conf the configuration, only respected for the first call
   * @param cacheManage cache manager
   * @param metadataCache metadata cache manager
   */
  public AlluxioCacheFileSystem(FileSystem fs, AlluxioConfiguration conf,
                                Optional<CacheManager> cacheManage,
                                Optional<MetadataCache> metadataCache) {
    super(fs);
    // Handle metrics
    MetricsSystem.startSinks(conf.getString(PropertyKey.METRICS_CONF_FILE));
    mCacheManager = Preconditions.checkNotNull(cacheManage, "cacheManager");
    mConf = Preconditions.checkNotNull(conf, "conf");
    if (mCacheManager.isPresent()) {
      mCacheFilter = Optional.of(CacheFilter.create(conf));
    } else {
      mCacheFilter = Optional.empty();
    }
    mMetadataCache = Preconditions.checkNotNull(metadataCache, "metadataCache");
    if (mCacheManager.isPresent()) {
      int masterClientThreads = mConf
          .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX);
      mDisableUpdateFileAccessTime = mConf
          .getBoolean(PropertyKey.USER_UPDATE_FILE_ACCESSTIME_DISABLED);
      // At a time point, there are at most the same number of concurrent master clients that
      // asynchronously update access time.
      mAccessTimeUpdater = new ThreadPoolExecutor(0, masterClientThreads, THREAD_KEEPALIVE_SECOND,
          TimeUnit.SECONDS, new SynchronousQueue<>());
      MetricsSystem.registerCachedGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.CLIENT_META_DATA_CACHE_SIZE.getName()),
          mMetadataCache.get()::size);
    } else {
      mAccessTimeUpdater = null;
      mDisableUpdateFileAccessTime = true;
    }
    LocalCacheFileInStream.registerMetrics();
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mDelegatedFileSystem.getConf();
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    if (mMetadataCache.isPresent()) {
      mMetadataCache.get().invalidate(path.getParent());
      mMetadataCache.get().invalidate(path);
    }
    mDelegatedFileSystem.createDirectory(path, options);
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws IOException, AlluxioException {
    if (mMetadataCache.isPresent()) {
      mMetadataCache.get().invalidate(path.getParent());
      mMetadataCache.get().invalidate(path);
    }
    return mDelegatedFileSystem.createFile(path, options);
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws IOException,
      AlluxioException {
    if (mMetadataCache.isPresent()) {
      mMetadataCache.get().invalidate(path.getParent());
      mMetadataCache.get().invalidate(path);
    }
    mDelegatedFileSystem.delete(path, options);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws IOException, AlluxioException {
    if (mMetadataCache.isPresent()) {
      mMetadataCache.get().invalidate(src.getParent());
      mMetadataCache.get().invalidate(src);
      mMetadataCache.get().invalidate(dst.getParent());
      mMetadataCache.get().invalidate(dst);
    }
    mDelegatedFileSystem.rename(src, dst, options);
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws IOException, AlluxioException {
    return mDelegatedFileSystem.getBlockLocations(getStatus(path));
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (mMetadataCache.isPresent()) {
      URIStatus status = mMetadataCache.get().get(path);
      if (status == null || !status.isCompleted()) {
        try {
          status = mDelegatedFileSystem.getStatus(path, options);
          mMetadataCache.get().put(path, status);
        } catch (FileDoesNotExistException e) {
          mMetadataCache.get().put(path, NOT_FOUND_STATUS);
          throw e;
        }
      } else if (status == NOT_FOUND_STATUS) {
        throw new FileDoesNotExistException("Path \"" + path.getPath() + "\" does not exist.");
      } else if (options.getUpdateTimestamps()) {
        // Asynchronously send an RPC to master to update the access time.
        // Otherwise, if we need to synchronously send RPC to master to do this,
        // caching the status does not bring any benefit.
        asyncUpdateFileAccessTime(path);
      }
      return status;
    } else {
      return mDelegatedFileSystem.getStatus(path, options);
    }
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
                            Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (options.getRecursive()) {
      // Do not cache results of recursive list status,
      // because some results might be cached multiple times.
      // Otherwise, needs more complicated logic inside the cache,
      // that might not worth the effort of caching.
      mDelegatedFileSystem.iterateStatus(path, options, action);
      return;
    }

    if (mMetadataCache.isPresent()) {
      List<URIStatus> cachedStatuses = mMetadataCache.get().listStatus(path);
      if (cachedStatuses == null) {
        List<URIStatus> statuses = new ArrayList<>();
        mDelegatedFileSystem.iterateStatus(path, options, status -> {
          statuses.add(status);
          action.accept(status);
        });
        mMetadataCache.get().put(path, statuses);
        return;
      }
      cachedStatuses.forEach(action);
    } else {
      mDelegatedFileSystem.iterateStatus(path, options, action);
    }
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    if (options.getRecursive()) {
      // Do not cache results of recursive list status,
      // because some results might be cached multiple times.
      // Otherwise, needs more complicated logic inside the cache,
      // that might not worth the effort of caching.
      return mDelegatedFileSystem.listStatus(path, options);
    }

    if (mMetadataCache.isPresent()) {
      List<URIStatus> statuses = mMetadataCache.get().listStatus(path);
      if (statuses == null) {
        statuses = mDelegatedFileSystem.listStatus(path, options);
        mMetadataCache.get().put(path, statuses);
      }
      return statuses;
    } else {
      return mDelegatedFileSystem.listStatus(path, options);
    }
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws IOException, AlluxioException {
    URIStatus status;
    if (mMetadataCache.isPresent()) {
      status = getStatus(path,
          FileSystemOptionsUtils.getStatusDefaults(mConf).toBuilder()
              .setAccessMode(Bits.READ)
              .setUpdateTimestamps(options.getUpdateLastAccessTime())
              .build());
    } else {
      status = mDelegatedFileSystem.getStatus(path);
    }
    if (!mCacheManager.isPresent()
        || mCacheManager.get().state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openFile(path, options);
    }
    return openFile(status, options);
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws IOException, AlluxioException {
    if (!mCacheManager.isPresent() || mCacheManager.get().state() == CacheManager.State.NOT_IN_USE
        || !mCacheFilter.get().needsCache(status)) {
      return mDelegatedFileSystem.openFile(status, options);
    }
    return new LocalCacheFileInStream(status,
        uriStatus -> mDelegatedFileSystem.openFile(status, options), mCacheManager.get(), mConf,
        Optional.empty());
  }

  @Override
  public PositionReader openPositionRead(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException {
    if (!mCacheManager.isPresent()
        || mCacheManager.get().state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openPositionRead(path, options);
    }
    try {
      return openPositionRead(mDelegatedFileSystem.getStatus(path), options);
    } catch (IOException | AlluxioException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  @Override
  public PositionReader openPositionRead(URIStatus status, OpenFilePOptions options) {
    if (!mCacheManager.isPresent()
        || mCacheManager.get().state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openPositionRead(status, options);
    }
    return LocalCachePositionReader.create(mConf, mCacheManager.get(),
        new CloseableSupplier<>(() -> mDelegatedFileSystem.openPositionRead(status, options)),
        status, mConf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE),
        status.getCacheContext() == null ? CacheContext.defaults() : status.getCacheContext());
  }

  @Override
  public synchronized void close() throws IOException {
    if (!mDelegatedFileSystem.isClosed()) {
      ThreadUtils.shutdownAndAwaitTermination(mAccessTimeUpdater, THREAD_TERMINATION_TIMEOUT_MS);
      mDelegatedFileSystem.close();
    }
  }

  /**
   * Asynchronously update file's last access time.
   *
   * @param path the path to the file
   */
  @VisibleForTesting
  public void asyncUpdateFileAccessTime(AlluxioURI path) {
    if (mDisableUpdateFileAccessTime) {
      return;
    }
    try {
      mAccessTimeUpdater.submit(() -> {
        try {
          AlluxioConfiguration conf = mConf;
          GetStatusPOptions getStatusOptions = FileSystemOptionsUtils
              .getStatusDefaults(conf).toBuilder()
              .setAccessMode(Bits.READ)
              .setUpdateTimestamps(true)
              .build();
          super.getStatus(path, getStatusOptions);
        } catch (IOException | AlluxioException e) {
          LOG.error("Failed to update access time for " + path, e);
        }
      });
    } catch (RejectedExecutionException e) {
      LOG.warn("Failed to submit a task to update access time for {}: {}", path, e.toString());
    }
  }

  /**
   * @return metadata cache size
   */
  public long getMetadataCacheSize() {
    return mMetadataCache.get().size();
  }
}
