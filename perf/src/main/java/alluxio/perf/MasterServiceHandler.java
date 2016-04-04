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

package alluxio.perf;

import org.apache.thrift.TException;

import alluxio.perf.thrift.MasterService;
import alluxio.perf.thrift.SlaveAlreadyRegisterException;
import alluxio.perf.thrift.SlaveNotRegisterException;

/**
 * The thrift server side of Alluxio-Perf Master.
 */
public class MasterServiceHandler implements MasterService.Iface {
  private final SlaveStatus mSlaveStatus;

  public MasterServiceHandler(SlaveStatus slaveStatus) {
    mSlaveStatus = slaveStatus;
  }

  @Override
  public boolean slave_canRun(int taskId, String nodeName) throws SlaveNotRegisterException,
      TException {
    return mSlaveStatus.allReady(taskId + "@" + nodeName);
  }

  @Override
  public void slave_finish(int taskId, String nodeName, boolean successFinish)
      throws SlaveNotRegisterException, TException {
    mSlaveStatus.slaveFinish(taskId + "@" + nodeName, successFinish);
  }

  @Override
  public void slave_ready(int taskId, String nodeName, boolean successSetup)
      throws SlaveNotRegisterException, TException {
    mSlaveStatus.slaveReady(taskId + "@" + nodeName, successSetup);
  }

  @Override
  public boolean slave_register(int taskId, String nodeName, String cleanupDir)
      throws SlaveAlreadyRegisterException, TException {
    return mSlaveStatus.slaveRegister(taskId + "@" + nodeName, cleanupDir);
  }
}
