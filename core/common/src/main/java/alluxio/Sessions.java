/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import alluxio.util.IdUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@code Sessions} represents and manages all sessions contacting a worker.
 */
@ThreadSafe
public class Sessions {
  private static final Logger LOG = LoggerFactory.getLogger(Sessions.class);

  public static final int MIGRATE_DATA_SESSION_ID = -3;
  public static final int MASTER_COMMAND_SESSION_ID = -4;
  public static final int CACHE_WORKER_SESSION_ID = -7;
  public static final int CACHE_UFS_SESSION_ID = -8;

  // internal session id base should be smaller than all predefined session ids
  public static final long INTERNAL_SESSION_ID_BASE = -8;

  /** Map from SessionId to {@link alluxio.SessionInfo} object. */
  private final Map<Long, SessionInfo> mSessions;

  /**
   * Creates a new instance of {@link Sessions}.
   */
  public Sessions() {
    mSessions = new HashMap<>();
  }

  /**
   * Gets the sessions that timed out.
   *
   * @return the list of session ids of sessions that timed out
   */
  public List<Long> getTimedOutSessions() {
    List<Long> ret = new ArrayList<>();
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
  public void removeSession(long sessionId) {
    LOG.debug("Cleaning up session {}", sessionId);
    synchronized (mSessions) {
      mSessions.remove(sessionId);
    }
  }

  /**
   * @return a session id used internally in workers
   */
  public static long createInternalSessionId() {
    return INTERNAL_SESSION_ID_BASE
        - IdUtils.getRandomNonNegativeLong() % (Long.MAX_VALUE + INTERNAL_SESSION_ID_BASE);
  }
}
