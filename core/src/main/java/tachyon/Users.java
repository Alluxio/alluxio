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

import tachyon.conf.CommonConf;
import tachyon.util.CommonUtils;

/**
 * <code>Users</code> represents and manages all users contacting to a worker.
 */
public class Users {
  public static final int DATASERVER_USER_ID = -1;
  public static final int CHECKPOINT_USER_ID = -2;
  public static final int MIGRATE_DATA_USER_ID = -3;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** User's temporary data folder in the under filesystem **/
  private final String mUserUnderFSFolder;
  /** Map from UserId to {@link tachyon.UserInfo} object **/
  private final Map<Long, UserInfo> mUsers;

  public Users(final String userUfsFolder) {

    mUserUnderFSFolder = userUfsFolder;
    mUsers = new HashMap<Long, UserInfo>();
  }

  /**
   * Check the status of the users pool.
   *
   * @return the list of timeout users.
   */
  public List<Long> checkStatus() {
    LOG.debug("Worker is checking all users' status.");
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
    return CommonUtils.concat(mUserUnderFSFolder, userId);
  }

  /**
   * Remove <code> userId </code> from user pool.
   *
   * @param userId The user to be removed.
   */
  public synchronized void removeUser(long userId) {
    StringBuilder sb = new StringBuilder("Trying to cleanup user " + userId + " : ");
    UserInfo tUser = null;
    synchronized (mUsers) {
      tUser = mUsers.get(userId);
      mUsers.remove(userId);
    }

    if (tUser == null) {
      sb.append(" The user does not exist in the worker's current user pool.");
    } else {
      String folder = getUserUfsTempFolder(userId);
      sb.append(" Remove users underfs folder ").append(folder);
      try {
        UnderFileSystem.get(CommonConf.get().UNDERFS_ADDRESS).delete(folder, true);
      } catch (IOException e) {
        LOG.warn(e.getMessage(), e);
      }
    }

    LOG.info(sb.toString());
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
        mUsers.put(userId, new UserInfo(userId));
      }
    }
  }
}
