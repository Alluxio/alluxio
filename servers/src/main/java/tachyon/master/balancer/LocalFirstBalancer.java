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

import java.util.Map;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.MasterWorkerInfo;
import tachyon.thrift.NetAddress;

/**
 * A default balancer that returns the local worker or a ramdom remote worker if
 * there is no local workers. This class serves as an example how to implement an
 * {@link Balancer}.
 */
public class LocalFirstBalancer implements Balancer {

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public LocalFirstBalancer() {
  }

  @Override
  public NetAddress getWorker(boolean random, String host,
      Map<Long, MasterWorkerInfo> mWorkers,
      Map<NetAddress , Long> mWorkerAddressToId) throws UnknownHostException {
    if (random) {
      int index = new Random()
          .nextInt(mWorkerAddressToId.size());
      for (NetAddress address : mWorkerAddressToId.keySet()) {
        if (index == 0) {
          LOG.debug("getRandomWorker: {}", address);
          return address;
        }
        index--;
      }
      for (NetAddress address : mWorkerAddressToId.keySet()) {
        LOG.debug("getRandomWorker: {}", address);
        return address;
      }
    } else {
      for (NetAddress address : mWorkerAddressToId.keySet()) {
        InetAddress inetAddress = InetAddress.getByName(address
            .getMHost());
        if (inetAddress.getHostName().equals(host)
            || inetAddress.getHostAddress().equals(host)
            || inetAddress.getCanonicalHostName().equals(host)) {
          LOG.debug("getLocalWorker: {}" + address);
          return address;
        }
      }
    }
    LOG.info("getLocalWorker: no local worker on " + host);
    return null;
  }
}
