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

package tachyon.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClient;
import tachyon.conf.TachyonConf;
import tachyon.thrift.UserMasterService;

/**
 * The UserMaster client, for clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety.
 */
public final class UserMasterClient extends MasterClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private UserMasterService.Client mClient = null;

  public UserMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.USER_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new UserMasterService.Client(mProtocol);
  }

  @Override
  protected void afterDisconnect() {}

  /**
   * Get a user id
   *
   * @return a new user id
   * @throws IOException
   */
  public synchronized long getUserId() throws IOException {
    while (!mIsClosed) {
      connect();
      try {
        return mClient.getUserId();
      } catch (TException e) {
        LOG.error(e.getMessage(), e);
        mConnected = false;
      }
    }
    return -1;
  }
}
