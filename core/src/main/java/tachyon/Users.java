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

import com.google.common.base.Throwables;

import tachyon.conf.CommonConf;
import tachyon.util.CommonUtils;

/**
 * <code>Users</code> represents and manages all users contacting to a worker.
 */
public class Users {
  public static final int sDATASERVER_USER_ID = -1;
  public static final int sCHECKPOINT_USER_ID = -2;

  private final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /** User's temporary data folder in the worker **/
  private final String USER_FOLDER;
  /** User's teomporary data folder in the under filesystem **/
  private final String USER_UFS_FOLDER;
  /** Map from UserId to {@link tachyon.UserInfo} object **/
  private final Map<Long, UserInfo> USERS;

  public Users(final String userfolder, final String userUfsFolder) {
    USER_FOLDER = userfolder;
    USER_UFS_FOLDER = userUfsFolder;
    USERS = new HashMap<Long, UserInfo>();
  }

  /**
   * Adds user's own bytes and updates the user's heartbeat.
   * 
   * @param userId
   *          id of the user.
   * @param newBytes
   *          delta bytes the user owns.
   */
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

  /**
   * Returns the user's temporary data folder in the worker's machine.
   * 
   * @param userId
   *          The queried user.
   * @return String contains user's temporary data folder in the worker's machine..
   */
  public String getUserTempFolder(long userId) {
    return CommonUtils.concat(USER_FOLDER, userId);
  }

  /**
   * Returns the user's temporary data folder in the under filesystem.
   * 
   * @param userId
   *          The queried user.
   * @return String contains the user's temporary data folder in the under filesystem.
   */
  public String getUserUfsTempFolder(long userId) {
    return CommonUtils.concat(USER_UFS_FOLDER, userId);
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
        throw Throwables.propagate(e);
      }

      folder = getUserUfsTempFolder(userId);
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

  /**
   * Updates user's heartbeat.
   * 
   * @param userId
   *          the id of the user
   */
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
