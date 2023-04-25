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
 * A thread used for Grpc messaging and stores a {@link GrpcMessagingContext} for current thread.
 * The context is storead in a {@link WeakReference} in order to allow the thread
 * to be garbage collected.
 *
 * There is no {@link GrpcMessagingContext} associated with the thread when it is first created.
 * It is the responsibility of thread creators to {@link #setContext(GrpcMessagingContext) set}
 * the thread context when appropriate.
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
    mContext = new WeakReference<>(context);
  }

  /**
   * @return the Grpc messaging context or null if no context has been configured
   */
  public GrpcMessagingContext getContext() {
    return mContext != null ? mContext.get() : null;
  }
}
