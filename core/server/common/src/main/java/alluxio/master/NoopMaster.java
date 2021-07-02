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

import alluxio.Server;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.NoopJournaled;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.underfs.UfsManager;

import java.util.Map;
import java.util.Set;

/**
 * Master implementation that does nothing. This is useful for testing and for situations where we
 * don't want to run a real master, e.g. when formatting the journal.
 */
public class NoopMaster implements Master, NoopJournaled {
  private final String mName;
  private final MasterContext mMasterContext;

  /**
   * Creates a new {@link NoopMaster}.
   */
  public NoopMaster() {
    this("NoopMaster");
  }

  /**
   * Creates a new {@link NoopMaster} with the given name.
   *
   * @param name the master name
   */
  public NoopMaster(String name) {
    this(name, new NoopUfsManager());
  }

  /**
   * Creates a new {@link NoopMaster} with the given name and ufsManager.
   *
   * @param name the master name
   * @param ufsManager the UFS manager
   */
  public NoopMaster(String name, UfsManager ufsManager) {
    mName = name;
    mMasterContext = new MasterContext(new NoopJournalSystem(), null, ufsManager);
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return null;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return null;
  }

  @Override
  public void start(Boolean options) {
  }

  @Override
  public void stop() {
  }

  @Override
  public void close() {
  }

  @Override
  public JournalContext createJournalContext() {
    throw new IllegalStateException("Cannot create journal contexts for NoopMaster");
  }

  @Override
  public MasterContext getMasterContext() {
    return mMasterContext;
  }
}
