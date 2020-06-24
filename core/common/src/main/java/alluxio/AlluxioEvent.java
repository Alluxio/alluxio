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

package alluxio;

import alluxio.collections.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Defines and Manages well-known Alluxio events.
 */
public enum AlluxioEvent {
  // [1000,2000) Alluxio master process events.
  MasterProcessCreated(1000),
  JournalSystemStarted(1001),
  JournalSystemStopped(1002),
  JournalSystemGainedPrimacy(1003),
  JournalSystemLostPrimacy(1004),
  MasterRpcServerStarted(1005),
  MasterRpcServerStopped(1006),
  MasterIsTransitioning(1007),
  //   [1100,1200) Alluxio FileSystemMaster events.
  ActiveSyncStarted(1100),
  ActiveSyncProcessedSyncPoint(1101),
  ActiveSyncFailed(1102),
  ActiveSyncFinished(1103),
  //   [1200,1300) Alluxio BlockMaster events.
  WorkerRegistered(1200),
  WorkerLost(1201),
  //   [1300,1400) Alluxio MetaMaster events.
  BackupRequested(1300),
  BackupStarted(1301),
  BackupSubmitted(1302),
  BackupFailed(1303),
  BackupFinished(1304),
  //   [1400,1500) Alluxio Table events.
  // [2000,3000) Alluxio worker process events.
  //   [2100,2200) Alluxio tier-management events.
  TierManagementTaskStarted(2100),
  TierManagementTaskFinished(2101),
  TierManagementTaskFailed(2102),
  BlockStoreEvictionFailed(2103)
  // [3000,4000) Alluxio job-master process events.
  // [4000,5000) Alluxio job-worker process events.
  // [5000,6000) Alluxio proxy process events.
  ;

  // logger.
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEvent.class);

  /** The unique event id. */
  private final int mId;

  /**
   * Creates new Alluxio event type.
   *
   * @param eventId the unique event id
   */
  AlluxioEvent(int eventId) {
    mId = eventId;
  }

  /**
   * @return the event id
   */
  public int getId() {
    return mId;
  }

  /**
   * Fires the event.
   *
   * It takes an optional list of pairs, each representing an argumentId-value pair.
   *
   * @param eventArguments additional event arguments to include in the event
   */
  public void fire(Pair<String, Object>... eventArguments) {
    StringBuilder eventStrBuilder = new StringBuilder();
    eventStrBuilder.append(this.name());
    eventStrBuilder.append(String.format("(%d)", mId));
    if (eventArguments.length > 0) {
      eventStrBuilder.append(" Event-Arguments: ");
      eventStrBuilder.append(Arrays.stream(eventArguments)
          .map((kv) -> String.format("%s=\"%s\"", kv.getFirst(), kv.getSecond()))
          .collect(Collectors.joining(", ")));
    }
    LOG.info(eventStrBuilder.toString());
  }
}
