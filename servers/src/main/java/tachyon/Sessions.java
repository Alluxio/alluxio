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

package tachyon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;

/**
 * <code>Sessions</code> represents and manages all session contacting a worker.
 */
public class Sessions {
  public static final int DATASERVER_SESSION_ID = -1;
  public static final int CHECKPOINT_SESSION_ID = -2;
  public static final int MIGRATE_DATA_SESSION_ID = -3;
  public static final int MASTER_COMMAND_SESSION_ID = -4;
  public static final int ACCESS_BLOCK_SESSION_ID = -5;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Session's temporary data folder in the under filesystem **/
  private final String mSessionUnderFSFolder;
  /** Map from SessionId to {@link tachyon.SessionInfo} object **/
  private final Map<Long, SessionInfo> mSessions;
  private final TachyonConf mTachyonConf;

  public Sessions(String sessionUfsFolder, TachyonConf tachyonConf) {
    mSessionUnderFSFolder = sessionUfsFolder;
    mSessions = new HashMap<Long, SessionInfo>();
    mTachyonConf = tachyonConf;
  }

  /**
   * Get the sessions that timed out.
   *
   * @return the list of session ids of sessions that timed out.
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
   * Returns the session's temporary data folder in the under filesystem.
   *
   * @param sessionId The queried session.
   * @return String contains the session's temporary data folder in the under filesystem.
   */
  public String getSessionUfsTempFolder(long sessionId) {
    return PathUtils.concatPath(mSessionUnderFSFolder, sessionId);
  }

  /**
   * Remove <code> sessionId </code> from session pool.
   *
   * @param sessionId The session to be removed.
   */
  public synchronized void removeSession(long sessionId) {
    LOG.info("Cleaning up session " + sessionId);
    SessionInfo tSession = null;
    synchronized (mSessions) {
      tSession = mSessions.remove(sessionId);
    }

    if (tSession == null) {
      LOG.warn("Session " + sessionId + " does not exist in the worker's current session pool.");
    } else {
      String folder = getSessionUfsTempFolder(sessionId);
      try {
        String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS, "/underFSStorage");
        UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, mTachyonConf);
        if (ufs.exists(folder)) {
          ufs.delete(folder, true);
        }
      } catch (IOException e) {
        LOG.warn("An error occurred removing the ufs folder of session " + sessionId, e);
      }
    }
  }

  /**
   * Updates session's heartbeat.
   *
   * @param sessionId the id of the session
   */
  public void sessionHeartbeat(long sessionId) {
    synchronized (mSessions) {
      if (mSessions.containsKey(sessionId)) {
        mSessions.get(sessionId).heartbeat();
      } else {
        int sessionTimeoutMs = mTachyonConf.getInt(Constants.WORKER_SESSION_TIMEOUT_MS);
        mSessions.put(sessionId, new SessionInfo(sessionId, sessionTimeoutMs));
      }
    }
  }
}
