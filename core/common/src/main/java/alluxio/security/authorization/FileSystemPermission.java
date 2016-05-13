/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authorization;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.ExceptionMessage;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A class for file/directory permissions.
 */
@NotThreadSafe
public final class FileSystemPermission {
  //POSIX permission style
  private FileSystemAction mUseraction;
  private FileSystemAction mGroupaction;
  private FileSystemAction mOtheraction;

  /**
   * Constructs an instance of {@link FileSystemPermission} with the given {@link FileSystemAction}.
   *
   * @param userFileSystemAction  user action
   * @param groupFileSystemAction group action
   * @param otherFileSystemAction other action
   */
  public FileSystemPermission(FileSystemAction userFileSystemAction,
      FileSystemAction groupFileSystemAction, FileSystemAction otherFileSystemAction) {
    set(userFileSystemAction, groupFileSystemAction, otherFileSystemAction);
  }

  /**
   * Constructs an instance of {@link FileSystemPermission} with the given mode.
   *
   * @param mode the digital representation of a {@link FileSystemPermission}
   * @see #toShort()
   */
  public FileSystemPermission(short mode) {
    fromShort(mode);
  }

  /**
   * Copy constructor.
   *
   * @param otherPermission another {@link FileSystemPermission}
   */
  public FileSystemPermission(FileSystemPermission otherPermission) {
    set(otherPermission.mUseraction, otherPermission.mGroupaction, otherPermission.mOtheraction);
  }

  /**
   * @return the user {@link FileSystemAction}
   */
  public FileSystemAction getUserAction() {
    return mUseraction;
  }

  /**
   * @param permission the digital representation of a {@link FileSystemPermission}
   * @return the user {@link FileSystemAction}
   */
  public static FileSystemAction createUserAction(short permission) {
    return FileSystemAction.values()[(permission >>> 6) & 7];
  }

  /**
   * @return the group {@link FileSystemAction}
   */
  public FileSystemAction getGroupAction() {
    return mGroupaction;
  }

  /**
   * @param permission the digital representation of a {@link FileSystemPermission}
   * @return the group {@link FileSystemAction}
   */
  public static FileSystemAction createGroupAction(short permission) {
    return FileSystemAction.values()[(permission >>> 3) & 7];
  }

  /**
   * @return the other {@link FileSystemAction}
   */
  public FileSystemAction getOtherAction() {
    return mOtheraction;
  }

  /**
   * @param permission the digital representation of a {@link FileSystemPermission}
   * @return the other {@link FileSystemAction}
   */
  public static FileSystemAction createOtherAction(short permission) {
    return FileSystemAction.values()[permission & 7];
  }

  private void set(FileSystemAction u, FileSystemAction g, FileSystemAction o) {
    mUseraction = u;
    mGroupaction = g;
    mOtheraction = o;
  }

  /**
   * Sets attributes of {@link FileSystemPermission} from its digital representation.
   *
   * @param n the digital representation of a {@link FileSystemPermission}
   */
  public void fromShort(short n) {
    FileSystemAction[] v = FileSystemAction.values();
    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7]);
  }

  /**
   * Encodes the object to a short.
   *
   * @return the digital representation of this {@link FileSystemPermission}
   */
  public short toShort() {
    int s = (mUseraction.ordinal() << 6) | (mGroupaction.ordinal() << 3) | mOtheraction.ordinal();
    return (short) s;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FileSystemPermission) {
      FileSystemPermission that = (FileSystemPermission) obj;
      return mUseraction == that.mUseraction && mGroupaction == that.mGroupaction && mOtheraction
          == that.mOtheraction;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toShort();
  }

  @Override
  public String toString() {
    return mUseraction.getSymbol() + mGroupaction.getSymbol() + mOtheraction.getSymbol();
  }

  /**
   * Applies a umask to this permission and return a new one.
   *
   * @param umask the umask {@link FileSystemPermission}
   * @return a new {@link FileSystemPermission}
   */
  public FileSystemPermission applyUMask(FileSystemPermission umask) {
    return new FileSystemPermission(mUseraction.and(umask.mUseraction.not()),
        mGroupaction.and(umask.mGroupaction.not()), mOtheraction.and(umask.mOtheraction.not()));
  }

  /**
   * Applies a umask to this permission and return a new one.
   *
   * @param conf the configuration to read the umask permission from
   * @return a new {@link FileSystemPermission}
   */
  public FileSystemPermission applyUMask(Configuration conf) {
    return applyUMask(getUMask(conf));
  }

  /**
   * Gets the default permission.
   *
   * @return the default {@link FileSystemPermission}
   */
  public static FileSystemPermission getDefault() {
    return new FileSystemPermission(Constants.DEFAULT_FS_FULL_PERMISSION);
  }

  /**
   * Gets the none permission for NOSASL authentication mode.
   *
   * @return the none {@link FileSystemPermission}
   */
  public static FileSystemPermission getNoneFsPermission() {
    return new FileSystemPermission(FileSystemAction.NONE, FileSystemAction.NONE,
        FileSystemAction.NONE);
  }

  /**
   * Gets the full permission for NOSASL authentication mode.
   *
   * @return the none {@link FileSystemPermission}
   */
  public static FileSystemPermission getFullFsPermission() {
    return new FileSystemPermission(FileSystemAction.ALL, FileSystemAction.ALL,
        FileSystemAction.ALL);
  }

  /**
   * Gets the file/directory creation umask.
   *
   * @param conf the runtime configuration of Alluxio
   * @return the umask {@link FileSystemPermission}
   */
  public static FileSystemPermission getUMask(Configuration conf) {
    int umask = Constants.DEFAULT_FS_PERMISSIONS_UMASK;
    if (conf != null) {
      String confUmask = conf.get(Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK);
      if (confUmask != null) {
        if ((confUmask.length() > 4) || !tryParseInt(confUmask)) {
          throw new IllegalArgumentException(ExceptionMessage.INVALID_CONFIGURATION_VALUE
              .getMessage(confUmask, Constants.SECURITY_AUTHORIZATION_PERMISSIONS_UMASK));
        }
        int newUmask = 0;
        int lastIndex = confUmask.length() - 1;
        for (int i = 0; i <= lastIndex; i++) {
          newUmask += (confUmask.charAt(i) - '0') << 3 * (lastIndex - i);
        }
        umask = newUmask;
      }
    }
    return new FileSystemPermission((short) umask);
  }

  private static boolean tryParseInt(String value) {
    try {
      Integer.parseInt(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
