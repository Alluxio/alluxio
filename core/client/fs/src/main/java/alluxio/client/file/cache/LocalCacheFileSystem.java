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

package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.client.file.DelegatingFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

/**
 * A FileSystem implementation with a local cache.
 */
public class LocalCacheFileSystem extends DelegatingFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheFileSystem.class);
  private static final Lock CACHE_INIT_LOCK = new ReentrantLock();
  @GuardedBy("CACHE_INIT_LOCK")
  private static final AtomicReference<CacheManager> CACHE_MANAGER = new AtomicReference<>();

  private final AlluxioConfiguration mConf;

  // TODO(binfan): Remove the IOException throwing or make this into a factory method
  /**
   * @param fs a FileSystem instance to query on local cache miss
   * @param conf the configuration, only respected for the first call
   */
  public LocalCacheFileSystem(FileSystem fs, AlluxioConfiguration conf) throws IOException {
    super(fs);
    // TODO(feng): support multiple cache managers
    if (CACHE_MANAGER.get() == null) {
      if (CACHE_INIT_LOCK.tryLock()) {
        try {
          if (CACHE_MANAGER.get() == null) {
            CACHE_MANAGER.set(CacheManager.create(conf));
          }
        } catch (IOException e) {
          Metrics.CREATE_ERRORS.inc();
          throw new IOException("Failed to create CacheManager", e);
        } finally {
          CACHE_INIT_LOCK.unlock();
        }
      }
    }
    mConf = conf;
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mDelegatedFileSystem.getConf();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws IOException, AlluxioException {
    if (CACHE_MANAGER.get() == null
        || CACHE_MANAGER.get().state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openFile(path, options);
    }
    return new LocalCacheFileInStream(path, options, mDelegatedFileSystem, CACHE_MANAGER.get());
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws IOException, AlluxioException {
    if (CACHE_MANAGER.get() == null
        || CACHE_MANAGER.get().state() == CacheManager.State.NOT_IN_USE) {
      return mDelegatedFileSystem.openFile(status, options);
    }
    return new LocalCacheFileInStream(status, options, mDelegatedFileSystem, CACHE_MANAGER.get());
  }

  private static final class Metrics {
    /** Errors when creating cache. */
    private static final Counter CREATE_ERRORS =
        MetricsSystem.counter(MetricKey.CLIENT_CACHE_CREATE_ERRORS.getName());

    private Metrics() {} // prevent instantiation
  }
}
