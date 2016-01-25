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

package tachyon.security.authorization;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;

/**
 * A class for file/directory permissions.
 */
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
  public FileSystemPermission applyUMask(TachyonConf conf) {
    return applyUMask(getUMask(conf));
  }

  /**
   * Gets the default permission.
   *
   * @return the default {@link FileSystemPermission}
   */
  public static FileSystemPermission getDefault() {
    return new FileSystemPermission(Constants.DEFAULT_TFS_FULL_PERMISSION);
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
   * Gets the file/directory creation umask.
   *
   * @param conf the runtime configuration of Tachyon
   * @return the umask {@link FileSystemPermission}
   */
  public static FileSystemPermission getUMask(TachyonConf conf) {
    int umask = Constants.DEFAULT_TFS_PERMISSIONS_UMASK;
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
