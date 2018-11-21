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

package alluxio.master.metastore.rocks;

import alluxio.master.metastore.BlockStore;
import alluxio.master.metastore.FileStore;
import alluxio.master.metastore.Metastore;
import alluxio.master.metastore.java.HeapFileStore;

import org.rocksdb.RocksDBException;

/**
 * Metastore backed by RocksDB.
 */
public class RocksMetastore implements Metastore {
  private final FileStore mFileStore;
  private final BlockStore mBlockStore;

  /**
   * Constructs a new RocksMetastore.
   */
  public RocksMetastore() throws RocksDBException {
    mFileStore = new HeapFileStore();
    mBlockStore = new RocksBlockStore();
  }

  @Override
  public FileStore getFileStore() {
    return mFileStore;
  }

  @Override
  public BlockStore getBlockStore() {
    return mBlockStore;
  }
}
