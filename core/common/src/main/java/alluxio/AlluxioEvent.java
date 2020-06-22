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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Defines and Manages well-known Alluxio events.
 */
public enum AlluxioEvent {
  // [1000,2000) Alluxio process events.
  MasterProcessStarting(1000, "Master process starting."),
  JournalSystemStarted(1001, "Journal system started."),
  MasterIsPrimary(1002, "Master process is now the primary."),
  MasterIsSecondary(1003, "Master process is now a secondary."),
  MasterIsTransitioning(1004, "Master process is transitioning to become."),
  MasterProcessStopping(1005, "Master process stopping."),
  // [2000,3000) Alluxio fs master events.
  // [3000,4000) Alluxio block master events.
  WorkerRegistered(3000, "Worker registered."),
  WorkerLost(3001, "Worker lost.");
  // [4000,5000) Alluxio meta master events.
  // [5000,6000) Alluxio table master events.

  // logger.
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEvent.class);

  /** The unique event id. */
  private final int mId;
  /** The message string format. */
  private final String mMessage;

  /**
   * Creates new Alluxio event type.
   *
   * @param eventId the unique event id
   * @param message the event message
   */
  AlluxioEvent(int eventId, String message) {
    mId = eventId;
    mMessage = message;
  }

  /**
   * @return the event id
   */
  public int getId() {
    return mId;
  }

  /**
   * @return the event message
   */
  public String getMessage() {
    return mMessage;
  }

  /**
   * Fires the event.
   *
   * @param additionalInfo additional information to include in the event
   */
  public void fire(Object... additionalInfo) {
    String eventString = String.format("Id: %d, Name: %s, Message:%s, AdditionalInfo: %s",
        mId, this.name(), mMessage,
        Arrays.stream(additionalInfo).map(Object::toString).collect(Collectors.joining(",")));
    LOG.info(eventString);
  }
}
