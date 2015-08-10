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

package tachyon.master.balancer;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.MasterWorkerInfo;
import tachyon.thrift.NetAddress;

/**
 * A Balancer that get the worker with most free space.
 */
public class MaxFreeBalancer implements Balancer {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public MaxFreeBalancer() {

  }

  @Override
  public NetAddress getWorker(boolean random, String host,
      Map<Long, MasterWorkerInfo> mWorkers,
      Map<NetAddress , Long> mWorkerAddressToId) throws UnknownHostException {
   
    long maxFreeSpace = Long.MIN_VALUE; 
    long freeSpace;
    NetAddress address = null; 
    for (MasterWorkerInfo workerInfo : mWorkers.values()) {
      freeSpace = Collections.max(workerInfo.getFreeBytesOnTiers());
      if (freeSpace > maxFreeSpace) {
        address = workerInfo.getAddress();
        maxFreeSpace = freeSpace;
      }
    }
    LOG.info("getWorker with MaxFreeBalancerStrategy: {}", address);
    return address;
  }
}
