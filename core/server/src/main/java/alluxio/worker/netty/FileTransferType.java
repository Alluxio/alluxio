/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker.netty;

import javax.annotation.concurrent.ThreadSafe;

/**
 * How a read response will transfer block data over the network. There is a difference in speed and
 * memory consumption between the two. {@link #MAPPED} is the default since at larger sizes it
 * outperforms {@link #TRANSFER}.
 */
@ThreadSafe
public enum FileTransferType {
  /**
   * Uses a {@link java.nio.MappedByteBuffer} to transfer data over the network.
   */
  MAPPED,

  /**
   * Uses {@link java.nio.channels.FileChannel#transferTo} to transfer data over the network.
   */
  TRANSFER
}
