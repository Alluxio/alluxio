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

package alluxio;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;
import javax.annotation.Nullable;

/**
 * Cache for storing file input and output streams.
 *
 * When a stream is added to the cache, the cache returns a unique ID that can be used for future
 * lookups of the stream. The streams are automatically closed when their cache entry is
 * invalidated or after being inactive for an extended period of time.
 */
@ThreadSafe
public final class StreamCache {
  private static final Logger LOG = LoggerFactory.getLogger(StreamCache.class);

  private static final RemovalListener<Integer, Closeable> CLOSER =
      removal -> {
        try {
          removal.getValue().close();
        } catch (Exception e) {
          LOG.error("Failed to close stream: ", e);
        }
      };

  private AtomicInteger mCounter;
  private Cache<Integer, FileInStream> mInStreamCache;
  private Cache<Integer, FileOutStream> mOutStreamCache;

  /**
   * Creates a new instance of {@link StreamCache}.
   *
   * @param timeoutMs the timeout (in milliseconds) for the stream cache eviction
   */
  public StreamCache(long timeoutMs) {
    mCounter = new AtomicInteger();
    mInStreamCache = CacheBuilder.newBuilder()
        .expireAfterAccess(timeoutMs, TimeUnit.MILLISECONDS).removalListener(CLOSER).build();
    mOutStreamCache = CacheBuilder.newBuilder()
        .expireAfterAccess(timeoutMs, TimeUnit.MILLISECONDS).removalListener(CLOSER).build();
  }

  /**
   * @param key the key to get the stream for
   * @return the stream or null if the cache contains no stream for the given key
   */
  public FileInStream getInStream(Integer key) {
    return mInStreamCache.getIfPresent(key);
  }

  /**
   * @param key the key to get the stream for
   * @return the stream or null if the cache contains no stream for the given key
   */
  public FileOutStream getOutStream(Integer key) {
    return mOutStreamCache.getIfPresent(key);
  }

  /**
   * @param is the stream to cache
   * @return the key for looking up the stream
   */
  public Integer put(FileInStream is) {
    int id = mCounter.incrementAndGet();
    mInStreamCache.put(id, is);
    return id;
  }

  /**
   * @param os the stream to cache
   * @return the key for looking up the stream
   */
  public Integer put(FileOutStream os) {
    int id = mCounter.incrementAndGet();
    mOutStreamCache.put(id, os);
    return id;
  }

  /**
   * @param key the key for the stream to invalidate
   * @return the invalidated stream or null if the cache contains no stream for the given key
   */
  @Nullable
  public Closeable invalidate(Integer key) {
    FileInStream is = mInStreamCache.getIfPresent(key);
    if (is != null) {
      mInStreamCache.invalidate(key);
      return is;
    }
    FileOutStream os = mOutStreamCache.getIfPresent(key);
    if (os != null) {
      mOutStreamCache.invalidate(key);
      return os;
    }
    return null;
  }

  /**
   * @return total size of the cache for both input streams and output streams
   */
  public long size() {
    return mInStreamCache.size() + mOutStreamCache.size();
  }
}
