/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker;

import java.io.Closeable;
import java.net.InetSocketAddress;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Defines how to interact with a server running the data protocol.
 */
public interface DataServer extends Closeable {

  class Factory {
    public static DataServer createDataServer(final InetSocketAddress dataAddress,
        final BlocksLocker blockLocker, TachyonConf conf) {
      try {
        return CommonUtils.createNewClassInstance(
            //conf.getClass(Constants.WORKER_DATA_SEVRER, Constants.WORKER_DATA_SERVER_CLASS),
            conf.getClass(Constants.WORKER_DATA_SEVRER, tachyon.worker.netty.NettyDataServer.class),
            new Class[] { InetSocketAddress.class, BlocksLocker.class, TachyonConf.class },
            new Object[] { dataAddress, blockLocker, conf });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  public abstract int getPort();

  public abstract boolean isClosed();
}
