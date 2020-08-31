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

public class GrpcMessagingThreadFactory implements ThreadFactory {
  private final AtomicInteger threadNumber = new AtomicInteger(1);
  private final String nameFormat;

  public GrpcMessagingThreadFactory(String nameFormat) {
    this.nameFormat = nameFormat;
  }

  public Thread newThread(Runnable r) {
    return new GrpcMessagingThread(r, String.format(this.nameFormat, this.threadNumber.getAndIncrement()));
  }
}