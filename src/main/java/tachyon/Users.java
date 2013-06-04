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

  public Users(String userfolder, String userHdfsFolder) {
    USER_FOLDER = userfolder;
    USER_UNDERFS_FOLDER = userHdfsFolder;

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

  public long ownBytes(long userId) {
    synchronized (USERS) {
      UserInfo tUser = USERS.get(userId);
      return tUser == null ? 0 : tUser.getOwnBytes();
    }
  }

  public List<Long> checkStatus(WorkerSpaceCounter workerSpaceCounter) {
    LOG.debug("Worker is checking all users' status.");
    List<Long> ret = new ArrayList<Long>();
    synchronized (USERS) {
      List<Long> toRemoveUsers = new ArrayList<Long>();
      for (Entry<Long, UserInfo> entry : USERS.entrySet()) {
        if (entry.getValue().timeout()) {
          toRemoveUsers.add(entry.getKey());
        }
      }

      for (Long id : toRemoveUsers) {
        workerSpaceCounter.returnUsedBytes(removeUser(id));
        ret.add(id);
      }
    }
    return ret;
  }

  public String getUserTempFolder(long userId) {
    return USER_FOLDER + "/" + userId;
  }

  public String getUserHdfsTempFolder(long userId) {
    return USER_UNDERFS_FOLDER + "/" + userId;
  }

  private long removeUser(long userId) {
    StringBuilder sb = new StringBuilder("Trying to cleanup user " + userId + " : ");
    UserInfo tUser = null;
    synchronized (USERS) {
      tUser = USERS.get(userId);
      USERS.remove(userId);
    }

    long ret = 0;
    if (tUser == null) {
      ret = 0;
      sb.append(" The user does not exist in the worker's current user pool.");
    } else {
      ret = tUser.getOwnBytes();
      String folder = getUserTempFolder(userId);
      sb.append(" The user returns " + ret + " bytes. Remove the user's folder " + folder + " ;");
      try {
        FileUtils.deleteDirectory(new File(folder));
      } catch (IOException e) {
        CommonUtils.runtimeException(e);
      }

      folder = getUserHdfsTempFolder(userId);
      sb.append(" Also remove users underfs folder " + folder);
      try {
        UnderFileSystem.getUnderFileSystem(CommonConf.get().UNDERFS_ADDRESS).delete(folder, true);
      } catch (IOException e) {
        LOG.error(e);
      }
    }

    LOG.info(sb.toString());
    return ret;
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