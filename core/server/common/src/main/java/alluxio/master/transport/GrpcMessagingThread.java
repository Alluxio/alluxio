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

package alluxio.master.transport;

import java.lang.ref.WeakReference;

/**
 * A thread used for Grpc messaging.
 */
public class GrpcMessagingThread extends Thread {
  private WeakReference<GrpcMessagingContext> mContext;

  /**
   * Constructs a new {@link GrpcMessagingThread}.
   *
   * @param target the target runnable
   * @param name name of the thread
   */
  public GrpcMessagingThread(Runnable target, String name) {
    super(target, name);
  }

  /**
   * Sets the Grpc messaging context.
   *
   * @param context context to set
   */
  public void setContext(GrpcMessagingContext context) {
    mContext = new WeakReference(context);
  }

  /**
   * Gets the Grpc context.
   *
   * @return the Grpc messaging context
   */
  public GrpcMessagingContext getContext() {
    return mContext != null ? mContext.get() : null;
  }
}
