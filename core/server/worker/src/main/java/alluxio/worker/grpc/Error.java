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

package alluxio.worker.grpc;

import alluxio.exception.status.AlluxioStatusException;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper on an error used to pass error information from the gRPC event thread to the data
 * reader thread.
 */
@ThreadSafe
class Error {
  final AlluxioStatusException mCause;
  final boolean mNotifyClient;

  public Error(AlluxioStatusException cause, boolean notifyClient) {
    mCause = cause;
    mNotifyClient = notifyClient;
  }

  /**
   * @return the cause of this error
   */
  public AlluxioStatusException getCause() {
    return mCause;
  }

  /**
   * @return whether to notify client
   */
  public boolean isNotifyClient() {
    return mNotifyClient;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("cause", mCause).add("notifyClient", mNotifyClient)
        .toString();
  }
}
