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

package alluxio.client.file.cache.store;

/**
 * This represents the different page store implementations that can be instantiated.
 */
public enum PageStoreType {
  /**
   * A simple store with pages on the local filesystem.
   */
  LOCAL(PageStoreType.LOCAL_OVERHEAD_RATIO),
  /**
   * A store that utilizes RocksDB to store and retrieve pages.
   */
  ROCKS(PageStoreType.ROCKS_OVERHEAD_RATIO),
  /**
   * A simple store with pages on the memory (HeapByteBuffer).
   */
  MEM(PageStoreType.MEMORY_OVERHEAD_RATIO);

  // We assume there will be some overhead using ByteBuffer as a page store,
  // i.e., with 1GB space allocated, we
  // expect no more than 1024MB / (1 + BUFF_MEMORY_OVERHEAD_RATIO) logical data stored
  private static final double MEMORY_OVERHEAD_RATIO = 0.1;
  // We assume 20% overhead using Rocksdb as a page store, i.e., with 1GB space allocated, we
  // expect no more than 1024MB/(1+20%)=853MB logical data stored
  private static final double ROCKS_OVERHEAD_RATIO = 0.2;
  // We assume there will be some overhead using local fs as a page store,
  // i.e., with 1GB space allocated, we
  // expect no more than 1024MB / (1 + LOCAL_OVERHEAD_RATIO) logical data stored
  private static final double LOCAL_OVERHEAD_RATIO = 0.05;
  private final double mOverheadRatio;

  /**
   * @param overheadRatio
   */
  PageStoreType(double overheadRatio) {
    mOverheadRatio = overheadRatio;
  }

  /**
   * @return overhead ratio
   */
  public double getOverheadRatio() {
    return mOverheadRatio;
  }
}
