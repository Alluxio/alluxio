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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI thread pool executors information.
 */
@NotThreadSafe
public final class WebUIThreadPoolExecutors implements Serializable {
  private static final long serialVersionUID = -1790282916933963383L;

  private boolean mDebug;
  private ThreadPoolExecutorInfo[] mThreadPoolExecutorInfo;

  /**
   * Creates a new instance of {@link WebUIThreadPoolExecutors}.
   */
  public WebUIThreadPoolExecutors() {
  }

  /**
   * Gets debug.
   *
   * @return the debug
   */
  public boolean getDebug() {
    return mDebug;
  }

  /**
   * Get thread pool executor info [ ].
   *
   * @return the thread pool executor info [ ]
   */
  public ThreadPoolExecutorInfo[] getThreadPoolExecutorInfo() {
    return mThreadPoolExecutorInfo;
  }

  /**
   * @param threadPoolExecutorInfo thread pool executor info [ ]
   */
  public void setThreadPoolExecutorInfo(ThreadPoolExecutorInfo[] threadPoolExecutorInfo) {
    mThreadPoolExecutorInfo = threadPoolExecutorInfo.clone();
  }

  /**
   * Sets debug.
   *
   * @param Debug the debug
   * @return the debug
   */
  public WebUIThreadPoolExecutors setDebug(boolean Debug) {
    mDebug = Debug;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("threadPoolExecutorInfo", mThreadPoolExecutorInfo)
        .toString();
  }
}
