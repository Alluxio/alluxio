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

package alluxio.worker.netty;

import javax.annotation.concurrent.ThreadSafe;

/**
 * How a read response will transfer block data over the network. There is a difference in speed and
 * memory consumption between the two. {@link #MAPPED} is the default since at larger sizes it
 * outperforms {@link #TRANSFER}.
 */
@ThreadSafe
enum FileTransferType {
  /**
   * Uses a {@link java.nio.MappedByteBuffer} to transfer data over the network.
   */
  MAPPED,

  /**
   * Uses {@link java.nio.channels.FileChannel#transferTo} to transfer data over the network.
   */
  TRANSFER
}
