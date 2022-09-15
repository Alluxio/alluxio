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
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * PendingPath in InodeSyncStream type.
 */
public interface InodeSyncStreamPendingPathCollection {
  /**
   * Size of collection.
   * @return size of collection
   */
  int size();

  /**
   * Check if the collection is empty.
   * @return isEmpty
   */
  boolean isEmpty();

  /**
   * Poll item from collection.
   * @return AlluxioURI
   */
  AlluxioURI poll();

  /**
   * Add item to collection.
   * @param uri
   * @return result of add
   */
  boolean add(AlluxioURI uri);

  /**
   * Check if the collection is queue.
   * @return is queue
   */
  boolean isQueue();

  /**
   * Factory method to create instance.
   * @return InodeSyncStreamPendingPathCollection
   */
  public static InodeSyncStreamPendingPathCollection createCollection() {
    InodeSyncStreamPendingPathCollectionType type =
        Configuration.getEnum(PropertyKey.MASTER_METADATA_SYNC_PENDING_PATH_TYPE,
            InodeSyncStreamPendingPathCollectionType.class);

    switch (type) {
      case STACK:
        return new Stack();
      default:
        return new Queue();
    }
  }

  /**
   * InodeSyncStreamPendingPathCollection.Queue.
   */
  public static class Queue implements InodeSyncStreamPendingPathCollection {
    private final ConcurrentLinkedQueue<AlluxioURI> mQueue = new ConcurrentLinkedQueue<>();

    @Override
    public int size() {
      return mQueue.size();
    }

    @Override
    public boolean isEmpty() {
      return mQueue.isEmpty();
    }

    @Override
    public AlluxioURI poll() {
      return mQueue.poll();
    }

    @Override
    public boolean add(AlluxioURI uri) {
      return mQueue.add(uri);
    }

    @Override
    public boolean isQueue() {
      return true;
    }
  }

  /**
   * InodeSyncStreamPendingPathCollection.STACK.
   */
  public static class Stack implements InodeSyncStreamPendingPathCollection {
    private final ConcurrentLinkedDeque<AlluxioURI> mDeque = new ConcurrentLinkedDeque<>();

    @Override
    public int size() {
      return mDeque.size();
    }

    @Override
    public boolean isEmpty() {
      return mDeque.isEmpty();
    }

    @Override
    public AlluxioURI poll() {
      return mDeque.pollLast();
    }

    @Override
    public boolean add(AlluxioURI uri) {
      return mDeque.add(uri);
    }

    @Override
    public boolean isQueue() {
      return false;
    }
  }
}
