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

package alluxio.master;

import alluxio.Configuration;
import alluxio.Process;
import alluxio.PropertyKey;
import alluxio.exception.NoMasterException;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalUtils;
import alluxio.thrift.RegisterMasterTOptions;
import alluxio.wire.ConfigProperty;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A master process in the Alluxio system.
 */
public interface MasterProcess extends Process {
  /**
   * Factory for creating {@link MasterProcess}.
   */
  @ThreadSafe
  final class Factory {
    /**
     * @return a new instance of {@link MasterProcess}
     */
    public static MasterProcess create() {
      URI journalLocation = JournalUtils.getJournalLocation();
      JournalSystem journalSystem =
          new JournalSystem.Builder().setLocation(journalLocation).build();
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        PrimarySelector primarySelector = PrimarySelector.Factory.createZkPrimarySelector();
        return new FaultTolerantAlluxioMasterProcess(journalSystem, primarySelector);
      }
      return new AlluxioMasterProcess(journalSystem);
    }

    private Factory() {} // prevent instantiation
  }

  /**
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get

   * @return the given master
   */
  <T extends Master> T getMaster(Class<T> clazz);

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
   * @return the master's web address, or null if the web server hasn't been started yet
   */
  InetSocketAddress getWebAddress();

  /**
   * @return true if Alluxio is running in safe mode, false otherwise
   */
  boolean isInSafeMode();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing();

  /**
   * @return configuration information list
   */
  List<ConfigProperty> getConfiguration();

  /**
   * Returns a master id for the given master, creating one if the master is new.
   *
   * @param hostname the master hostname
   * @return the master id for this master
   */
  long getMasterId(String hostname);

  /**
   * Updates metadata when a standby master periodically heartbeats with the leader master.
   *
   * @param masterId the master id
   * @return whether the master should re-register
   */
  boolean masterHeartbeat(long masterId);

  /**
   * Updates metadata when a standby master registers with the leader master.
   *
   * @param masterId the master id of the standby master registering
   * @param options the options that contains master configuration
   * @throws NoMasterException if masterId cannot be found
   */
  void masterRegister(long masterId, RegisterMasterTOptions options) throws NoMasterException;
}
