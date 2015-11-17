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

package tachyon.master;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.worker.block.BlockWorker;

public abstract class AbstractLocalTachyonCluster {
  protected static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected long mWorkerCapacityBytes;
  protected int mUserBlockSize;

  protected TachyonConf mMasterConf;
  protected TachyonConf mWorkerConf;

  protected BlockWorker mWorker = null;

  protected String mTachyonHome;
  protected String mHostname = null;

  protected Thread mWorkerThread = null;

  public AbstractLocalTachyonCluster(long workerCapacityBytes, int userBlockSize) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mUserBlockSize = userBlockSize;
  }

  public abstract TachyonFileSystem getClient() throws IOException;

  public abstract int getMasterPort();

  public TachyonConf getMasterTachyonConf() {
    return mMasterConf;
  }

  public abstract void start() throws IOException, ConnectionFailedException;

  protected void resetContext() {}

  protected void setHostname() {
    mHostname = NetworkAddressUtils.getLocalHostName(100);
  }

  protected void setTachyonHome() throws IOException {
    mTachyonHome =
        File.createTempFile("Tachyon", "U" + System.currentTimeMillis()).getAbsolutePath();
  }

  public void stop() throws Exception {
    stopTFS();
    stopUFS();

    resetContext();
  }

  public abstract void stopTFS() throws Exception;

  public abstract void stopUFS() throws Exception;
}
