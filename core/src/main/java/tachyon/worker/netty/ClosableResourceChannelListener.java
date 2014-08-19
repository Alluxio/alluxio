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
package tachyon.worker.netty;

import java.io.Closeable;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

/**
 * A listener that will close the given resource when the operation completes.  This class
 * accepts null resources.
 */
final class ClosableResourceChannelListener implements ChannelFutureListener {
  private final Closeable resource;

  ClosableResourceChannelListener(Closeable resource) {
    this.resource = resource;
  }

  @Override
  public void operationComplete(ChannelFuture future) throws Exception {
    if (resource != null) {
      resource.close();
    }
  }
}
