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

import alluxio.master.CallTracker;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.LinkedList;
import java.util.List;

/**
 * Used as a base class for wrapping context around proto messages.
 *
 * @param <T> Proto message type
 */
@NotThreadSafe
public class OperationContext<T extends com.google.protobuf.GeneratedMessageV3.Builder<?>> {
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
   * Adds a new call-tracker to this context.
   *
   * @param callTracker the call tracker
   */
  public void addCallTracker(CallTracker callTracker) {
    synchronized (mCallTrackers) {
      mCallTrackers.add(Preconditions.checkNotNull(callTracker));
    }
  }

  /**
   * @return {@code true} if the call is cancelled by the client
   */
  public boolean isCancelled() {
    synchronized (mCallTrackers) {
      if (mCallTrackers.isEmpty()) {
        throw new IllegalStateException("No tracker registered.");
      }
      for (CallTracker callTracker : mCallTrackers) {
        if (callTracker.isCancelled()) {
          return true;
        }
      }
      return false;
    }
  }
}
