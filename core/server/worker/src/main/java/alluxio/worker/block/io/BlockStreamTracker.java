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

package alluxio.worker.block.io;

import alluxio.worker.block.BlockStoreLocation;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Used to emit block reader/writer open/close events.
 */
public class BlockStreamTracker {
  /** List of listeners for this tracker. */
  private static List<BlockClientListener> sListeners = new CopyOnWriteArrayList<>();

  /**
   * Register a new listener.
   *
   * @param listener listener
   */
  public static void registerListener(BlockClientListener listener) {
    sListeners.add(listener);
  }

  /**
   * Unregisters a listener.
   *
   * @param listener listener
   */
  public static void unregisterListener(BlockClientListener listener) {
    sListeners.remove(listener);
  }

  /**
   * Called with a new block reader.
   *
   * @param reader block reader
   * @param location location of read
   */
  public static void readerOpened(BlockReader reader, BlockStoreLocation location) {
    for (BlockClientListener listener : sListeners) {
      listener.clientOpened(reader, location);
    }
  }

  /**
   * Called when an existing block reader is closed.
   *
   * @param reader block reader
   * @param location location of read
   */
  public static void readerClosed(BlockReader reader, BlockStoreLocation location) {
    for (BlockClientListener listener : sListeners) {
      listener.clientClosed(reader, location);
    }
  }

  /**
   * Called with a new block writer.
   *
   * @param writer block writer
   * @param location location of write
   */
  public static void writerOpened(BlockWriter writer, BlockStoreLocation location) {
    for (BlockClientListener listener : sListeners) {
      listener.clientOpened(writer, location);
    }
  }

  /**
   * Called when an existing block writer is closed.
   *
   * @param writer block writer
   * @param location location of write
   */
  public static void writerClosed(BlockWriter writer, BlockStoreLocation location) {
    for (BlockClientListener listener : sListeners) {
      listener.clientClosed(writer, location);
    }
  }
}
