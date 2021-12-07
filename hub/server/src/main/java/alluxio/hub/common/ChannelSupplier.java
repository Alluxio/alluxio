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

package alluxio.hub.common;

import alluxio.exception.status.AlluxioStatusException;

import io.grpc.Channel;

import java.net.InetSocketAddress;

/**
 * An interface which represents a {@link java.util.function.Function} that returns
 * {@link Channel} and may throw an {@link AlluxioStatusException}.
 */
@FunctionalInterface
public interface ChannelSupplier {

  /**
   * Creates a channel for the given socket address.
   *
   * @param addr the address to create a channel for
   * @return Create and return a {@link Channel}
   * @throws AlluxioStatusException if the channel can't be created
   */
  Channel get(InetSocketAddress addr) throws AlluxioStatusException;
}
