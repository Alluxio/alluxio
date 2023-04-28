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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The factory to create new {@link GrpcMessagingThread} with name.
 */
public class GrpcMessagingThreadFactory implements ThreadFactory {
  private final AtomicInteger mThreadNumber = new AtomicInteger(1);
  private final String mNameFormat;

  /**
   * Constructs a new {@link GrpcMessagingThreadFactory} which names thread.
   *
   * @param nameFormat the name format of this factory
   */
  public GrpcMessagingThreadFactory(String nameFormat) {
    mNameFormat = nameFormat;
  }

  @Override
  public Thread newThread(Runnable r) {
    return new GrpcMessagingThread(r,
        String.format(mNameFormat, mThreadNumber.getAndIncrement()));
  }
}
