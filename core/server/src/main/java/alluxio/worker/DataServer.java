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

package alluxio.worker;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Throwables;

import java.io.Closeable;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Defines how to interact with a server running the data protocol.
 */
public interface DataServer extends Closeable {

  /**
   * Factory for {@link DataServer}.
   */
  @ThreadSafe
  class Factory {
    /**
     * Factory for {@link DataServer}.
     *
     * @param dataAddress the address of the data server
     * @param blockWorker block worker handle
     * @param conf Alluxio configuration
     * @return the generated {@link DataServer}
     */
    public static DataServer create(final InetSocketAddress dataAddress,
        final BlockWorker blockWorker, Configuration conf) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<DataServer>getClass(Constants.WORKER_DATA_SERVER),
            new Class[] { InetSocketAddress.class, BlockWorker.class, Configuration.class },
            new Object[] { dataAddress, blockWorker, conf });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Gets the actual bind hostname on {@link DataServer} service.
   *
   * @return the bind host
   */
  String getBindHost();

  /**
   * Gets the port the {@link DataServer} is listening on.
   *
   * @return the port number
   */
  int getPort();

  /**
   * Checks if the {@link DataServer} is closed.
   *
   * @return true if the {@link DataServer} is closed, false otherwise
   */
  boolean isClosed();
}
