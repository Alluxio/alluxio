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

import alluxio.conf.PropertyKey;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.BackupPOptions;
import alluxio.grpc.ConfigProperties;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.MetaCommand;
import alluxio.grpc.RegisterMasterPOptions;
import alluxio.master.Master;
import alluxio.wire.Address;
import alluxio.wire.BackupResponse;
import alluxio.wire.ConfigCheckReport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The interface of meta master.
 */
public interface MetaMaster extends Master {

  /**
   * Backs up the master.
   *
   * @param options method options
   * @return the uri of the created backup
   */
  BackupResponse backup(BackupPOptions options) throws IOException;

  /**
   * @return the server-side configuration checker report
   */
  ConfigCheckReport getConfigCheckReport();

  /**
   * @param options method options
   * @return configuration information list
   */
  List<ConfigProperty> getConfiguration(GetConfigurationPOptions options);

  /**
   * @param options get configuration options
   * @return a mapping from path to path level properties
   */
  Map<String, ConfigProperties> getPathConfiguration(GetConfigurationPOptions options);

  /**
   * Sets properties for a path.
   *
   * @param path the path
   * @param properties the properties for path
   */
  void setPathConfiguration(String path, Map<PropertyKey, String> properties) throws UnavailableException;

  /**
   * Removes properties for a path.
   *
   * @param path the path
   * @param keys the property keys
   */
  void removePathConfiguration(String path, Set<PropertyKey> keys) throws UnavailableException;

  /**
   * Removes all properties for a path.
   *
   * @param path the path
   */
  void removePathConfiguration(String path) throws UnavailableException;

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
  void masterRegister(long masterId, RegisterMasterPOptions options) throws NotFoundException;
}
