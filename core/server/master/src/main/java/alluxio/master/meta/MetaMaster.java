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
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.MetaCommand;
import alluxio.grpc.RegisterMasterPOptions;
import alluxio.master.Master;
import alluxio.master.backup.BackupOps;
import alluxio.wire.Address;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.ConfigHash;
import alluxio.wire.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The interface of meta master.
 */
public interface MetaMaster extends BackupOps, Master {

  /**
   * @return the cluster ID
   */
  String getClusterID();

  /**
   * @return the server-side configuration checker report
   */
  ConfigCheckReport getConfigCheckReport();

  /**
   * @param options method options
   * @return configuration information list
   */
  Configuration getConfiguration(GetConfigurationPOptions options);

  /**
   * @return hashes of cluster and path level configuration
   */
  ConfigHash getConfigHash();

  /**
   * @return the journal space monitor if supported by the current configuration
   */
  Optional<JournalSpaceMonitor> getJournalSpaceMonitor();

  /**
   * Sets properties for a path.
   *
   * @param path the path
   * @param properties the properties for path
   */
  void setPathConfiguration(String path, Map<PropertyKey, String> properties)
      throws UnavailableException;

  /**
   * Removes properties for a path.
   *
   * @param path the path
   * @param keys the property keys
   */
  void removePathConfiguration(String path, Set<String> keys) throws UnavailableException;

  /**
   * Removes all properties for a path.
   *
   * @param path the path
   */
  void removePathConfiguration(String path) throws UnavailableException;

  /**
   * Sets whether newer version Alluxio is available.
   *
   * @param available new version is available
   */
  void setNewerVersionAvailable(boolean available);

  /**
   * @return true if newer version is available, false otherwise
   */
  boolean getNewerVersionAvailable();

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

  /**
   * Creates a checkpoint in the primary master journal system.
   *
   * @return the hostname of the master that did the checkpoint
   */
  String checkpoint() throws IOException;

  /**
   * @param propertiesMap properties to update
   * @return the update properties status map
   */
  Map<String, Boolean> updateConfiguration(Map<String, String> propertiesMap);
}
