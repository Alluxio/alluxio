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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.file.URIStatus;
import alluxio.wire.BlockInfo;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implements the file meta cache
 */
@ThreadSafe
public class MetaCache {
  //private final Logger LOG = LoggerFactory.getLogger(MetaCache.class);

  private static LoadingCache<String, MetaCacheData> fcache = null;
  private static LoadingCache<Long, BlockInfoData> bcache = null;
  private static MetaCache singleton = new MetaCache();     // just make compiler happy

  public MetaCache() {
      int maxCachedPaths = Configuration.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
      fcache = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new MetaCacheLoader());
      bcache = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new BlockInfoLoader());
  }

  public static URIStatus getStatus(String path) {
      MetaCacheData c = fcache.getIfPresent(path);
      return (c != null) ? c.getStatus() : null;
  }

  public static void setStatus(String path, URIStatus s) {
      MetaCacheData c = fcache.getUnchecked(path);
      if (c != null) c.setStatus(s);
  }

  public static AlluxioURI getURI(String path) {
      MetaCacheData c = fcache.getUnchecked(path); //confirm to original code logic
      return (c != null) ? c.getURI() : null;
  }

  public static void invalidate(String path) {
      //also invalidate block belonging to the file
      MetaCacheData data = fcache.getIfPresent(path);
      if (data != null) {
          URIStatus stat = data.getStatus();
          if (stat != null) {
              for (long blockId: stat.getBlockIds()) {
                  bcache.invalidate(blockId);
              }
          }
      }
      fcache.invalidate(path);
  }

  public static void invalidatePrefix(String prefix) {
      Set<String> keys = fcache.asMap().keySet();
      for (String k: keys) {
          if (k.startsWith(prefix)) invalidate(k);
      }
  }

  public static void invalidateAll() {
      fcache.invalidateAll();
  }

  public static void addBlockInfoCache(long blockId, BlockInfo info) {
      BlockInfoData data = bcache.getUnchecked(blockId);
      if (data != null) data.setBlockInfo(info);
  }

  public static BlockInfo getBlockInfoCache(long blockId) {
      BlockInfoData b = bcache.getIfPresent(blockId);
      return (b != null) ? b.getBlockInfo() : null;
  }

  public static void invalidateBlockInfoCache(long blockId) {
      bcache.invalidate(blockId);
  }

  public static void invalidateAllBlockInfoCache() {
      bcache.invalidateAll();
  }

  public class MetaCacheData {
      private String path;
      private URIStatus uriStatus;
      private AlluxioURI uri;

      public MetaCacheData(String path) {
          this.path = path;
          this.uriStatus = null;
          this.uri = new AlluxioURI(this.path); // confirm to original code logic
      }

      public URIStatus getStatus() {
          return this.uriStatus;
      }

      public void setStatus(URIStatus s) {
          this.uriStatus = s;
      }

      public AlluxioURI getURI() {
          return this.uri;
      }
  }

  public class BlockInfoData {
      Long id;
      private BlockInfo info;

      BlockInfoData(Long id) {
          this.id = id;
          this.info = null;
      }

      public void setBlockInfo(BlockInfo info) {
          this.info = info;
      }
      public BlockInfo getBlockInfo() {
          return this.info;
      }
  }

  private final class MetaCacheLoader extends CacheLoader<String, MetaCacheData> {

    /**
     * Constructs a new {@link MetaCacheLoader}.
     */
    public MetaCacheLoader() {}

    @Override
    public MetaCacheData load(String path) {
      return new MetaCacheData(path);
    }
  }

  private final class BlockInfoLoader extends CacheLoader<Long, BlockInfoData> {

    /**
     * Constructs a new {@link BlockInfoLoader}.
     */
    public BlockInfoLoader() {}

    @Override
    public BlockInfoData load(Long blockid) {
      return new BlockInfoData(blockid);
    }
  }

}
