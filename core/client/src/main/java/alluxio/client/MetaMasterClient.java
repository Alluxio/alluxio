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

package alluxio.client;

import alluxio.exception.ConnectionFailedException;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import java.io.Closeable;
import java.util.List;

/**
 * Interface for the client to the meta master.
 */
public interface MetaMasterClient extends Closeable {
  /**
   * @param masterInfoFields optional list of fields to query; if null all fields will be queried
   * @return the requested master info
   * @throws ConnectionFailedException if the connection fails during the call
   */
  MasterInfo getInfo(List<MasterInfoField> masterInfoFields) throws ConnectionFailedException;
}
