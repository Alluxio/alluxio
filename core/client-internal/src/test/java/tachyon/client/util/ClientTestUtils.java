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

package tachyon.client.util;

import org.powermock.reflect.Whitebox;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.client.block.BlockStoreContext;
import tachyon.client.file.FileSystemContext;
import tachyon.client.lineage.LineageContext;

/**
 * Utility methods for the client tests.
 */
public final class ClientTestUtils {

  /**
   * Sets small buffer sizes so that Tachyon does not run out of heap space.
   */
  public static void setSmallBufferSizes() {
    ClientContext.getConf().set(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES, "4KB");
    ClientContext.getConf().set(Constants.USER_FILE_BUFFER_BYTES, "4KB");
  }

  /**
   * Reverts the client context configuration to the default value, and reinitializes all contexts
   * while rely on this configuration.
   *
   * This method should only be used as a cleanup mechanism between tests. It should not be used
   * while any object may be using the {@link ClientContext}.
   */
  public static void resetClientContext() {
    try {
      Whitebox.invokeMethod(ClientContext.class, "reset");
      resetContexts();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Re-initializes the {@link ClientContext} singleton to pick up any configuration changes.
   *
   * This method is needed when the master address has been changed so that new clients will use the
   * new address. It should not be used while any object may be using the {@link ClientContext}.
   */
  public static void reinitializeClientContext() {
    try {
      Whitebox.invokeMethod(ClientContext.class, "init");
      resetContexts();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static void resetContexts() {
    BlockStoreContext.INSTANCE.reset();
    FileSystemContext.INSTANCE.reset();
    LineageContext.INSTANCE.reset();
  }
}
