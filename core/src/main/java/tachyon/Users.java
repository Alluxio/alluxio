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
package tachyon;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import tachyon.conf.CommonConf;
import tachyon.util.CommonUtils;

/**
 * <code>Users</code> represents and manages all users contacting to a worker.
 */
public class Users {
  public static final int sDATASERVER_USER_ID = -1;
  public static final int sCHECKPOINT_USER_ID = -2;

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final String USER_FOLDER;
  private final String USER_UNDERFS_FOLDER;
  private final Map<Long, UserInfo> USERS;

  public Users(String userfolder, String userUnderfsFolder) {
    USER_FOLDER = userfolder;
    USER_UNDERFS_FOLDER = userUnderfsFolder;

    USERS = new HashMap<Long, UserInfo>();
  }

  public void addOwnBytes(long userId, long newBytes) {
    UserInfo tUser = null;
    synchronized (USERS) {
      userHeartbeat(userId);
      tUser = USERS.get(userId);
    }

    tUser.addOwnBytes(newBytes);
  }

  /**
   * Check the status of the users pool.
   * 
   * @return the list of timeout users.
   */
  public List<Long> checkStatus() {
    LOG.debug("Worker is checking all users' status.");
    List<Long> ret = new ArrayList<Long>();
    synchronized (USERS) {
      for (Entry<Long, UserInfo> entry : USERS.entrySet()) {
        if (entry.getValue().timeout()) {
          ret.add(entry.getKey());
        }
      }
    }
    return ret;
  }

  public String getUserTempFolder(long userId) {
    return CommonUtils.concat(USER_FOLDER, userId);
  }

  public String getUserUnderfsTempFolder(long userId) {
    return CommonUtils.concat(USER_UNDERFS_FOLDER, userId);
  }

  /**
   * Get how much space quote does a user own.
   * 
   * @param userId
   *          The queried user.
   * @return Bytes the user owns.
   */
  public long ownBytes(long userId) {
    synchronized (USERS) {
      UserInfo tUser = USERS.get(userId);
      return tUser == null ? 0 : tUser.getOwnBytes();
    }
  }

  /**
   * Remove <code> userId </code> from user pool.
   * 
   * @param userId
   *          The user to be removed.
   * @return The space quote the removed user occupied in bytes.
   */
  public synchronized long removeUser(long userId) {
    StringBuilder sb = new StringBuilder("Trying to cleanup user " + userId + " : ");
    UserInfo tUser = null;
    synchronized (USERS) {
      tUser = USERS.get(userId);
      USERS.remove(userId);
    }

    long returnedBytes = 0;
    if (tUser == null) {
      returnedBytes = 0;
      sb.append(" The user does not exist in the worker's current user pool.");
    } else {
      returnedBytes = tUser.getOwnBytes();
      String folder = getUserTempFolder(userId);
      sb.append(" The user returns " + returnedBytes + " bytes. Remove the user's folder "
          + folder + " ;");
      try {
        FileUtils.deleteDirectory(new File(folder));
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }

      folder = getUserUnderfsTempFolder(userId);
      sb.append(" Also remove users underfs folder " + folder);
      try {
        UnderFileSystem.get(CommonConf.get().UNDERFS_ADDRESS).delete(folder, true);
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    LOG.info(sb.toString());
    return returnedBytes;
  }

  public void userHeartbeat(long userId) {
    synchronized (USERS) {
      if (USERS.containsKey(userId)) {
        USERS.get(userId).heartbeat();
      } else {
        USERS.put(userId, new UserInfo(userId));
      }
    }
  }
}
