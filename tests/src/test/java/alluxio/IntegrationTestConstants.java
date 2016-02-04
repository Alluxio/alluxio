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

package alluxio;

/**
 * Constants for integration tests.
 */
public class IntegrationTestConstants {
  // DataServer variations.
  public static final String NETTY_DATA_SERVER = "alluxio.worker.netty.NettyDataServer";
  public static final String NIO_DATA_SERVER = "alluxio.worker.nio.NIODataServer";

  // Remote block reader variations.
  public static final String NETTY_BLOCK_READER = "alluxio.client.netty.NettyRemoteBlockReader";

  // Netty transfer types.
  public static final String MAPPED_TRANSFER = "MAPPED";
  public static final String FILE_CHANNEL_TRANSFER = "TRANSFER";
  public static final String UNUSED_TRANSFER = "UNUSED";
}
