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

package alluxio.master.meta;

import alluxio.exception.status.NotFoundException;
import alluxio.master.Master;
import alluxio.master.meta.checkconf.ServerConfigurationChecker;
import alluxio.thrift.MetaCommand;
import alluxio.thrift.RegisterMasterTOptions;
import alluxio.wire.Address;
import alluxio.wire.ConfigProperty;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * The interface of meta master.
 */
public interface MetaMaster extends Master {

  /**
   * @return the server-side configuration checker report
   */
  ServerConfigurationChecker.ConfigCheckReport getConfigCheckReport();

  /**
   * @return configuration information list
   */
  List<ConfigProperty> getConfiguration();

  /**
   * @return the addresses of live masters
   */
  List<Address> getMasterAddresses();

  /**
   * Returns a master id for the given master, creating one if the master is new.
   *
   * @param address the master hostname
   * @return the master id for this master
   */
  long getMasterId(Address address);

  /**
   * @return this master's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the master in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the master in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master's web port
   */
  int getWebPort();

  /**
   * @return the addresses of live workers
   */
  List<Address> getWorkerAddresses();

  /**
   * @return true if Alluxio is running in safe mode, false otherwise
   */
  boolean isInSafeMode();

  /**
   * A standby master periodically heartbeats with the leader master.
   *
   * @param masterId the master id
   * @return an optional command for the standby master to execute
   */
  MetaCommand masterHeartbeat(long masterId);

  /**
   * A standby master registers with the leader master.
   *
   * @param masterId the master id of the standby master registering
   * @param options the options that contains master configuration
   * @throws NotFoundException if masterId cannot be found
   */
  void masterRegister(long masterId, RegisterMasterTOptions options) throws NotFoundException;
}
