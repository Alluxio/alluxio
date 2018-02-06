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

package alluxio.security.authentication;

import alluxio.exception.status.UnauthenticatedException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

/**
 * Class for creating Thrift protocols for communicating with Alluxio services.
 */
public final class TProtocols {

  /**
   * @param transport a transport for communicating with an Alluxio Thrift server
   * @param serviceName the service to communicate with
   * @return a Thrift protocol for communicating with the given service through the transport
   */
  public static TProtocol createProtocol(TTransport transport, String serviceName)
      throws UnauthenticatedException {
    TProtocol binaryProtocol = new TBinaryProtocol(transport);
    TProtocol multiplexedProtocol = new TMultiplexedProtocol(binaryProtocol, serviceName);
    return multiplexedProtocol;
  }

  private TProtocols() {} // not intended for instantiation
}
