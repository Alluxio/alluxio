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

package alluxio.worker;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

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

    private Factory() {} // prevent instantiation

    /**
     * Factory for {@link DataServer}.
     *
     * @param dataAddress the address of the data server
     * @param worker the Alluxio worker handle
     * @return the generated {@link DataServer}
     */
    public static DataServer create(final SocketAddress dataAddress,
        final WorkerProcess worker) {
      try {
        return CommonUtils.createNewClassInstance(
            Configuration.<DataServer>getClass(PropertyKey.WORKER_DATA_SERVER_CLASS),
            new Class[] {SocketAddress.class, WorkerProcess.class},
            new Object[] {dataAddress, worker});
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Gets the actual bind socket address. It is either a {@link InetSocketAddress} or a
   * {@link io.netty.channel.unix.DomainSocketAddress}.
   *
   * @return the bind socket address
   */
  SocketAddress getBindAddress();

  /**
   * Checks if the {@link DataServer} is closed.
   *
   * @return true if the {@link DataServer} is closed, false otherwise
   */
  boolean isClosed();
}
