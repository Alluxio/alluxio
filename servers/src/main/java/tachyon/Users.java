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
 * <code>Users</code> represents and manages all users contacting a worker.
 */
public class Users {
  public static final int DATASERVER_USER_ID = -1;
  public static final int CHECKPOINT_USER_ID = -2;
  public static final int MIGRATE_DATA_USER_ID = -3;
  public static final int MASTER_COMMAND_USER_ID = -4;
  public static final int ACCESS_BLOCK_USER_ID = -5;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** User's temporary data folder in the under filesystem **/
  private final String mUserUnderFSFolder;
  /** Map from UserId to {@link tachyon.UserInfo} object **/
  private final Map<Long, UserInfo> mUsers;
  private final TachyonConf mTachyonConf;

  public Users(String userUfsFolder, TachyonConf tachyonConf) {
    mUserUnderFSFolder = userUfsFolder;
    mUsers = new HashMap<Long, UserInfo>();
    mTachyonConf = tachyonConf;
  }

  /**
   * Get the users that timed out.
   *
   * @return the list of user ids of users that timed out.
   */
  public List<Long> getTimedOutUsers() {
    LOG.debug("Worker is checking all users' status for timeouts.");
    List<Long> ret = new ArrayList<Long>();
    synchronized (mUsers) {
      for (Entry<Long, UserInfo> entry : mUsers.entrySet()) {
        if (entry.getValue().timeout()) {
          ret.add(entry.getKey());
        }
      }
    }
    return ret;
  }

  /**
   * Returns the user's temporary data folder in the under filesystem.
   *
   * @param userId The queried user.
   * @return String contains the user's temporary data folder in the under filesystem.
   */
  public String getUserUfsTempFolder(long userId) {
    return PathUtils.concatPath(mUserUnderFSFolder, userId);
  }

  /**
   * Remove <code> userId </code> from user pool.
   *
   * @param userId The user to be removed.
   */
  public synchronized void removeUser(long userId) {
    LOG.info("Cleaning up user " + userId);
    UserInfo tUser = null;
    synchronized (mUsers) {
      tUser = mUsers.remove(userId);
    }

    if (tUser == null) {
      LOG.warn("User " + userId + " does not exist in the worker's current user pool.");
    } else {
      String folder = getUserUfsTempFolder(userId);
      try {
        String ufsAddress = mTachyonConf.get(Constants.UNDERFS_ADDRESS, "/underFSStorage");
        UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, mTachyonConf);
        if (ufs.exists(folder)) {
          ufs.delete(folder, true);
        }
      } catch (IOException e) {
        LOG.warn("An error occurred removing the ufs folder of user " + userId, e);
      }
    }
  }

  /**
   * Updates user's heartbeat.
   *
   * @param userId the id of the user
   */
  public void userHeartbeat(long userId) {
    synchronized (mUsers) {
      if (mUsers.containsKey(userId)) {
        mUsers.get(userId).heartbeat();
      } else {
        int userTimeoutMs =
            mTachyonConf.getInt(Constants.WORKER_USER_TIMEOUT_MS);
        mUsers.put(userId, new UserInfo(userId, userTimeoutMs));
      }
    }
  }
}
