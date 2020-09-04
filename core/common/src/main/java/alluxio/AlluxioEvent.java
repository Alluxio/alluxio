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
 *
 * TODO(ggezer): Add per-process log4j configuration before adding events to other processes.
 */
public enum AlluxioEvent {
  // Alluxio events.
  // Next id:1019.
  MasterProcessCreated(1000),
  JournalSystemStarted(1001),
  JournalSystemStopped(1002),
  JournalSystemGainedPrimacy(1003),
  JournalSystemLostPrimacy(1004),
  MasterRpcServerStarted(1005),
  MasterRpcServerStopped(1006),
  MasterIsTransitioning(1007),
  WorkerRegistered(1008),
  WorkerLost(1009),
  BackupRequested(1010),
  BackupStarted(1011),
  BackupSubmitted(1012),
  BackupFailed(1013),
  BackupFinished(1014),
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
      eventStrBuilder.append(" Arguments: ");
      eventStrBuilder.append(Arrays.stream(eventArguments)
          .map((kv) -> String.format("%s=\"%s\"", kv.getFirst(), kv.getSecond()))
          .collect(Collectors.joining(", ")));
    }
    LOG.info(eventStrBuilder.toString());
  }
}
