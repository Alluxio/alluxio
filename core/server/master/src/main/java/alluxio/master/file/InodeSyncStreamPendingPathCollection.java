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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * The class of pendingPath in InodeSyncStream.
 */
public abstract class InodeSyncStreamPendingPathCollection {
  protected final ConcurrentLinkedDeque<AlluxioURI> mCollection = new ConcurrentLinkedDeque<>();

  /**
   * Size of collection.
   * @return size of collection
   */
  public int size() {
    return mCollection.size();
  }

  /**
   * Check if the collection is empty.
   * @return isEmpty
   */
  public boolean isEmpty() {
    return mCollection.isEmpty();
  }

  /**
   * Poll item from collection.
   * @return AlluxioURI
   */
  public abstract AlluxioURI poll();

  /**
   * Add item to collection.
   * @param uri
   * @return result of add
   */
  public boolean add(AlluxioURI uri) {
    return mCollection.add(uri);
  }

  /**
   * Factory method to create instance.
   * @return InodeSyncStreamPendingPathCollection
   */
  public static InodeSyncStreamPendingPathCollection createCollection() {
    MasterMetadataSyncTraverseType type =
        Configuration.getEnum(PropertyKey.MASTER_METADATA_SYNC_TRAVERSE_TYPE,
            MasterMetadataSyncTraverseType.class);

    switch (type) {
      case DFS:
        return new Stack();
      default:
        return new Queue();
    }
  }

  /**
   * InodeSyncStreamPendingPathCollection.Queue.
   */
  private static class Queue extends InodeSyncStreamPendingPathCollection {
    @Override
    public AlluxioURI poll() {
      return mCollection.poll();
    }
  }

  /**
   * InodeSyncStreamPendingPathCollection.STACK.
   */
  private static class Stack extends InodeSyncStreamPendingPathCollection {
    @Override
    public AlluxioURI poll() {
      return mCollection.pollLast();
    }
  }
}
