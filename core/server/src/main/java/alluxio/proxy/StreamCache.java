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

package alluxio.proxy;

import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

/**
 * Stream cache.
 */
public final class StreamCache {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final RemovalListener<Integer, Closeable> CLOSER =
      new RemovalListener<Integer, Closeable>() {
        public void onRemoval(@Nonnull RemovalNotification<Integer, Closeable> removal) {
          try {
            Closeable stream = removal.getValue();
            if (stream != null) {
              stream.close();
            }
          } catch (Exception e) {
            LOG.error("Failed to close stream: ", e);
          }
        }
      };

  private static Cache<Integer, FileInStream> sInStreamCache =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).removalListener(CLOSER)
          .build();
  private static Cache<Integer, FileOutStream> sOutStreamCache =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).removalListener(CLOSER)
          .build();
  private static AtomicInteger sCounter = new AtomicInteger();

  /**
   * @param key the key to get the stream for
   * @return the stream
   */
  public static FileInStream getInStream(Integer key) {
    return sInStreamCache.getIfPresent(key);
  }

  /**
   * @param key the key to get the stream for
   * @return the stream
   */
  public static FileOutStream getOutStream(Integer key) {
    return sOutStreamCache.getIfPresent(key);
  }

  /**
   * @param is the stream to cache
   * @return the key for looking up the stream
   */
  public static Integer put(FileInStream is) {
    int id = sCounter.incrementAndGet();
    sInStreamCache.put(id, is);
    return id;
  }

  /**
   * @param os the stream to cache
   * @return the key for looking up the stream
   */
  public static Integer put(FileOutStream os) {
    int id = sCounter.incrementAndGet();
    sOutStreamCache.put(id, os);
    return id;
  }

  /**
   * @param key the key for the stream to invalidate
   * @return the invalidated stream
   */
  public static Closeable invalidate(Integer key) {
    FileInStream is = sInStreamCache.getIfPresent(key);
    if (is != null) {
      sInStreamCache.invalidate(key);
      return is;
    }
    FileOutStream os = sOutStreamCache.getIfPresent(key);
    if (os != null) {
      sOutStreamCache.invalidate(key);
      return os;
    }
    return null;
  }

  private StreamCache() {} // prevent instantiation
}
