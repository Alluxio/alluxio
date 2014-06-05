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
package tachyon.master;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;

/**
 * Heartbeat executor for master client.
 */
class MasterClientHeartbeatExecutor implements HeartbeatExecutor {
  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  private final MasterClient CLIENT;
  private final long MAX_NONE_ACCESS_INTERVAL;

  public MasterClientHeartbeatExecutor(MasterClient client, long maxNoneAccessIntervalMs) {
    CLIENT = client;
    MAX_NONE_ACCESS_INTERVAL = maxNoneAccessIntervalMs;
  }

  @Override
  public void heartbeat() {
    long internalMs = System.currentTimeMillis() - CLIENT.getLastAccessedMs();
    if (internalMs > MAX_NONE_ACCESS_INTERVAL) {
      LOG.debug("The last Heartbeat was " + internalMs + " ago.");
      CLIENT.cleanConnect();
    }
  }
}
