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
package tachyon.worker;

import com.google.common.base.Throwables;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.util.CommonUtils;

/**
 * User client sends periodical heartbeats to the worker it is talking to. It is fails to do so,
 * the worker may withdraw the space granted to the particular user.
 */
class WorkerClientHeartbeatExecutor implements HeartbeatExecutor {
  private final WorkerClient WORKER_CLIENT;
  private final long USER_ID;

  public WorkerClientHeartbeatExecutor(WorkerClient workerClient, long userId) {
    WORKER_CLIENT = workerClient;
    USER_ID = userId;
  }

  @Override
  public void heartbeat() {
    try {
      WORKER_CLIENT.userHeartbeat(USER_ID);
    } catch (TException e) {
      throw Throwables.propagate(e);
    }
  }
}