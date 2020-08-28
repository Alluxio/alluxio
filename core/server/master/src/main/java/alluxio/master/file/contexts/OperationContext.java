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

package alluxio.master.file.contexts;

import com.google.protobuf.GeneratedMessageV3;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Used as a base class for wrapping context around proto messages.
 *
 * @param <T> Proto message type
 * @param <C> extended type
 */
@NotThreadSafe
public class OperationContext<T extends GeneratedMessageV3.Builder, C extends OperationContext> {
  // Proto message that is being wrapped
  private T mOptionsBuilder;
  // Used to track client call status.
  private List<CallTracker> mCallTrackers;

  /**
   * Creates an instance with given proto message.
   *
   * @param optionsBuilder Internal proto message builder instance
   */
  public OperationContext(T optionsBuilder) {
    mOptionsBuilder = optionsBuilder;
    mCallTrackers = new LinkedList<>();
  }

  /**
   * @return underlying proto message instance
   */
  public T getOptions() {
    return mOptionsBuilder;
  }

  /**
   * Updates this context with a new tracker.
   *
   * @param tracker the new call tracker
   * @return the updated instance
   */
  public C withTracker(CallTracker tracker) {
    mCallTrackers.add(tracker);
    return (C) this;
  }

  /**
   * @return the list of trackers that have cancelled this operation
   */
  public List<CallTracker> getCancelledTrackers() {
    boolean trackerCancelled = false;
    for (CallTracker tracker : mCallTrackers) {
      if (tracker.isCancelled()) {
        trackerCancelled = true;
        break;
      }
    }

    if (!trackerCancelled) {
      return Collections.emptyList();
    }

    List<CallTracker> cancelledTrackers = new LinkedList<>();
    for (CallTracker tracker : mCallTrackers) {
      if (tracker.isCancelled()) {
        cancelledTrackers.add(tracker);
      }
    }

    return cancelledTrackers;
  }
}
