/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.util;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockStoreContext;
import alluxio.client.file.FileSystemContext;
import alluxio.client.lineage.LineageContext;

import com.google.common.base.Throwables;
import org.powermock.reflect.Whitebox;

/**
 * Utility methods for the client tests.
 */
public final class ClientTestUtils {

  /**
   * Sets small buffer sizes so that Alluxio does not run out of heap space.
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
