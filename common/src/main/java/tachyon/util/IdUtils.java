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

package tachyon.util;

import java.util.UUID;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.block.BlockId;

/**
 * Utility methods for working with an id in Tachyon.
 */
@ThreadSafe
public final class IdUtils {
  private IdUtils() {} // prevent instantiation

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  public static final long INVALID_FILE_ID = -1;
  public static final long INVALID_WORKER_ID = -1;

  /**
   * Creates an id for a file based on the given id of the container.
   *
   * @param containerId the id of the container
   * @return a file id based on the given container id
   */
  public static long createFileId(long containerId) {
    long id = BlockId.createBlockId(containerId, BlockId.getMaxSequenceNumber());
    if (id == INVALID_FILE_ID) {
      // Right now, there's not much we can do if the file id we're returning is -1, since the file
      // id is completely determined by the container id passed in. However, by the current
      // algorithm, -1 will be the last file id generated, so the chances somebody will get to that
      // are slim. For now we just log it.
      LOG.warn("Created file id -1, which is invalid");
    }
    return id;
  }

  /**
   * @return the created RPC id
   */
  public static String createRpcId() {
    return UUID.randomUUID().toString();
  }
}
