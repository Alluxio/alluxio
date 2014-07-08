/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.Serializable;

/**
 * Class to store global counter in master's write head log and checkpoint file.
 */
public class Counters implements Serializable, Comparable<Counters> {
  private static final long serialVersionUID = -8873733429025713755L;

  private int mInodeCounter;
  private long mEditTransactionId;
  private int mDependencyCounter;

  public Counters(int inodeCounter, long editTransactionId, int dependencyCounter) {
    mInodeCounter = inodeCounter;
    mEditTransactionId = editTransactionId;
    mDependencyCounter = dependencyCounter;
  }

  @Override
  public synchronized int compareTo(Counters o) {
    if (mInodeCounter != o.mInodeCounter) {
      return mInodeCounter - o.mInodeCounter;
    }
    if (mEditTransactionId != o.mEditTransactionId) {
      return mEditTransactionId > o.mEditTransactionId ? 1 : -1;
    }
    if (mDependencyCounter == o.mDependencyCounter) {
      return 0;
    }
    return mDependencyCounter > o.mDependencyCounter ? 1 : -1;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof Counters)) {
      return false;
    }
    return compareTo((Counters) o) == 0;
  }

  public synchronized int getDependencyCounter() {
    return mDependencyCounter;
  }

  public synchronized long getEditTransactionCounter() {
    return mEditTransactionId;
  }

  public synchronized int getInodeCounter() {
    return mInodeCounter;
  }

  @Override
  public synchronized int hashCode() {
    return (int) (mInodeCounter + mEditTransactionId + mDependencyCounter);
  }

  public synchronized void updateDependencyCounter(int dependencyCounter) {
    mDependencyCounter = Math.max(mDependencyCounter, dependencyCounter);
  }

  public synchronized void updateEditTransactionCounter(long id) {
    mEditTransactionId = Math.max(mEditTransactionId, id);
  }

  public synchronized void updateInodeCounter(int inodeCounter) {
    mInodeCounter = Math.max(mInodeCounter, inodeCounter);
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Counter(").append(mInodeCounter).append(",")
        .append(mEditTransactionId).append(",").append(mDependencyCounter).append(")").toString();
  }
}