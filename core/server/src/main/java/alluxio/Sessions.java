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

package alluxio;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.conf.TachyonConf;
import alluxio.worker.WorkerContext;

/**
 * {@code Sessions} represents and manages all sessions contacting a worker.
 */
@ThreadSafe
public class Sessions {
  public static final int DATASERVER_SESSION_ID = -1;
  public static final int CHECKPOINT_SESSION_ID = -2;
  public static final int MIGRATE_DATA_SESSION_ID = -3;
  public static final int MASTER_COMMAND_SESSION_ID = -4;
  public static final int ACCESS_BLOCK_SESSION_ID = -5;
  public static final int KEYVALUE_SESSION_ID = -6;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Map from SessionId to {@link alluxio.SessionInfo} object **/
  private final Map<Long, SessionInfo> mSessions;

  /**
   * Creates a new instance of {@link Sessions}.
   */
  public Sessions() {
    mSessions = new HashMap<Long, SessionInfo>();
  }

  /**
   * Gets the sessions that timed out.
   *
   * @return the list of session ids of sessions that timed out
   */
  public List<Long> getTimedOutSessions() {
    LOG.debug("Worker is checking all sessions' status for timeouts.");
    List<Long> ret = new ArrayList<Long>();
    synchronized (mSessions) {
      for (Entry<Long, SessionInfo> entry : mSessions.entrySet()) {
        if (entry.getValue().timeout()) {
          ret.add(entry.getKey());
        }
      }
    }
    return ret;
  }

  /**
   * Removes the given session from the session pool.
   *
   * @param sessionId the id of the session to be removed
   */
  public synchronized void removeSession(long sessionId) {
    LOG.info("Cleaning up session {}", sessionId);
    synchronized (mSessions) {
      mSessions.remove(sessionId);
    }
  }

  /**
   * Performs session heartbeat.
   *
   * @param sessionId the id of the session
   */
  public void sessionHeartbeat(long sessionId) {
    synchronized (mSessions) {
      if (mSessions.containsKey(sessionId)) {
        mSessions.get(sessionId).heartbeat();
      } else {
        TachyonConf conf = WorkerContext.getConf();
        int sessionTimeoutMs = conf.getInt(Constants.WORKER_SESSION_TIMEOUT_MS);
        mSessions.put(sessionId, new SessionInfo(sessionId, sessionTimeoutMs));
      }
    }
  }
}
