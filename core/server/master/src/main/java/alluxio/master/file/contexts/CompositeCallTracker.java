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

import java.util.Arrays;
import java.util.List;

/**
 * A call-tracker that is composed of other call-trackers.
 */
public class CompositeCallTracker implements CallTracker {
  /** Call trackers. */
  private final List<CallTracker> mCallTrackers;

  /**
   * A new composite call-tracker that combines many call-trackers.
   *
   * @param callTrackers array of call-trackers
   */
  public CompositeCallTracker(CallTracker... callTrackers) {
    mCallTrackers = Arrays.asList(callTrackers);
  }

  @Override
  public boolean isCancelled() {
    for (CallTracker callTracker : mCallTrackers) {
      if (callTracker.isCancelled()) {
        return true;
      }
    }
    return false;
  }
}
