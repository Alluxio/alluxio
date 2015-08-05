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
import java.util.Map;

import tachyon.master.MasterWorkerInfo;
import tachyon.thrift.NetAddress;

/**
 * Interface for the balancing policy on workers for write.
 */
public interface Balancer {

  /**
   * 
   *
   * @param random If true, select a random worker
   * @param host If <code>random</code> is false, select a worker on this host
   * @param mWorkers the map which link the worker id to his workerInfo
   * @param mWorkerAddressToId the map which link the worker NetAddress to his id
   * @return the NetAdress of the worker selected 
   * @throws UnknownHostException
   */
  NetAddress getWorker(boolean random, String host,  
      Map<Long, MasterWorkerInfo> mWorkers, Map<NetAddress , Long> mWorkerAddressToId) 
    throws UnknownHostException;
}
