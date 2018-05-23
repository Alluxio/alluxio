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
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.nio.file.Path;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Implements the file meta cache
 */
@ThreadSafe
public class MetaCache {
  //private final Logger LOG = LoggerFactory.getLogger(MetaCache.class);

  private static Path alluxioRootPath = null;
  private static int maxCachedPaths = Configuration.getInt(PropertyKey.FUSE_CACHED_PATHS_MAX);
  private static LoadingCache<String, MetaCacheData> fcache 
      = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new MetaCacheLoader());
  private static LoadingCache<Long, BlockInfoData> bcache 
      = CacheBuilder.newBuilder().maximumSize(maxCachedPaths).build(new BlockInfoLoader());
  private static List<WorkerInfo> workerList = new ArrayList<>();
  private static long lastWorkerListAccess = 0;
  private static boolean attr_cache_enabled = true;
  private static boolean block_cache_enabled = true;
  private static boolean worker_cache_enabled = true;

  public static void setAlluxioRootPath(Path path) {
      alluxioRootPath = path;
  }

  public static void debug_meta_cache(String p) {
      if (p.startsWith("start_attr_cache")) {
          MetaCache.set_attr_cache(1);
          System.out.println("Alluxio attr cache enabled.");
      } else if (p.startsWith("stop_attr_cache")) {
          MetaCache.set_attr_cache(0);
          System.out.println("Alluxio attr cache disabled.");
      } else if (p.startsWith("show_attr_cache")) {
          System.out.println("Alluxio attr cache state:" + attr_cache_enabled);
          System.out.println("Alluxio attr cache size:" + fcache.size());
      } else if (p.startsWith("purge_attr_cache")) {
          MetaCache.set_attr_cache(2);
          System.out.println("Alluxio attr cache purged.");
      } else if (p.startsWith("start_block_cache")) {
          MetaCache.set_block_cache(1);
          System.out.println("Alluxio block cache enabled.");
      } else if (p.startsWith("stop_block_cache")) {
          MetaCache.set_block_cache(0);
          System.out.println("Alluxio block cache disabled.");
      } else if (p.startsWith("purge_block_cache")) {
          MetaCache.set_block_cache(2);
          System.out.println("Alluxio block cache purged.");
      } else if (p.startsWith("show_block_cache")) {
          System.out.println("Alluxio block cache state:" + block_cache_enabled);
          System.out.println("Alluxio block cache size:" + bcache.size());
      } else if (p.startsWith("start_worker_cache")) {
          MetaCache.set_worker_cache(1);
          System.out.println("Alluxio worker cache enabled.");
      } else if (p.startsWith("stop_worker_cache")) {
          MetaCache.set_worker_cache(0);
          System.out.println("Alluxio worker cache disabled.");
      } else if (p.startsWith("purge_worker_cache")) {
          MetaCache.set_worker_cache(2);
          System.out.println("Alluxio worker cache purged.");
      } else if (p.startsWith("show_worker_cache")) {
          System.out.println("Cached workers state:" + worker_cache_enabled);
          System.out.println("Cached workers:" + MetaCache.getWorkerInfoList());
      } else {
          p = p.replace("@", "/");
          if (p.startsWith("/")) {
              System.out.println("Attr cache for " + p + ":" + MetaCache.getStatus(p));
          } else {
              Long l = Long.parseLong(p);
              System.out.println("Cached block for " + l + ":" + MetaCache.getBlockInfoCache(l));
          }
      }
  }

  public static void set_attr_cache(int v) {
      switch (v) {
          case 0: 
              attr_cache_enabled = false; 
              fcache.invalidateAll();
              return;
          case 1: 
              attr_cache_enabled = true; 
              return;
          default: 
              fcache.invalidateAll(); 
      }
  }
  public static void set_block_cache(int v) {
      switch (v) {
          case 0: 
              block_cache_enabled = false;
              bcache.invalidateAll();
              return;
          case 1:
              block_cache_enabled = true;
              return;
          default:
              bcache.invalidateAll();
      }

  }
  public static void set_worker_cache(int v) {
      worker_cache_enabled = (0 == v) ? false : true;
      if (v > 1) MetaCache.invalidateWorkerInfoList();
  }

  public static void setWorkerInfoList(List<WorkerInfo> list) {
      if (!worker_cache_enabled) return;

      if (list != null) {
          synchronized (workerList) {
              workerList.clear();
              workerList.addAll(list);
          }
      }
  }

  public static List<WorkerInfo> getWorkerInfoList() {
      long now = System.currentTimeMillis(); // expire every 10s
      if (now - lastWorkerListAccess > 1000 * 300) {
          synchronized (workerList) {
              workerList.clear();
          }
      }
      lastWorkerListAccess = now;
      return workerList;
  }

  public static void invalidateWorkerInfoList() {
      synchronized (workerList) {
          workerList.clear();
      }
  }

  public static URIStatus getStatus(String path) {
      MetaCacheData c = fcache.getIfPresent(path);
      return (c != null) ? c.getStatus() : null;
  }

  public static boolean shouldUpdateStatus(URIStatus s) {
      if (s.isFolder() || s.getBlockSizeBytes() == 0) return false;
      if (s.getLength() == 0) return true;
      if (s.getInAlluxioPercentage() == 100) return false;
      return true;
  }

  public static void setStatus(String path, URIStatus s) {
      if (!attr_cache_enabled) return;

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
      if (!block_cache_enabled) return;

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

  static class MetaCacheData {
      private URIStatus uriStatus;
      private AlluxioURI uri;

      public MetaCacheData(String path) {
          if (alluxioRootPath != null) {
              Path tpath = alluxioRootPath.resolve(path.substring(1));
              this.uri = new AlluxioURI(tpath.toString());
          } else {
              this.uri = new AlluxioURI(path);
          }
          this.uriStatus = null;
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

  static class BlockInfoData {
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

  private static class MetaCacheLoader extends CacheLoader<String, MetaCacheData> {

    /**
     * Constructs a new {@link MetaCacheLoader}.
     */
    public MetaCacheLoader() {}

    @Override
    public MetaCacheData load(String path) {
      return new MetaCacheData(path);
    }
  }

  private static class BlockInfoLoader extends CacheLoader<Long, BlockInfoData> {

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
