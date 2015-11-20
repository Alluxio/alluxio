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
public final class FsPermission {
  //POSIX permission style
  private FsAction mUseraction;
  private FsAction mGroupaction;
  private FsAction mOtheraction;

  /**
   * Constructs an instance of {@link FsPermission} with the given {@link FsAction}.
   * @param userFsAction user action
   * @param groupFsAction group action
   * @param otherFsAction other action
   */
  public FsPermission(FsAction userFsAction, FsAction groupFsAction, FsAction otherFsAction) {
    set(userFsAction, groupFsAction, otherFsAction);
  }

  /**
   * Constructs an instance of {@link FsPermission} with the given mode.
   * @param mode
   * @see #toShort()
   */
  public FsPermission(short mode) {
    fromShort(mode);
  }

  /**
   * Copy constructor.
   * @param otherPermission
   */
  public FsPermission(FsPermission otherPermission) {
    set(otherPermission.mUseraction, otherPermission.mGroupaction, otherPermission.mOtheraction);
  }

  /**
   * Get user action
   * @return the user {@link FsAction}
   */
  public FsAction getUserAction() {
    return mUseraction;
  }

  /**
   * Get group action
   * @return the group {@link FsAction}
   */
  public FsAction getGroupAction() {
    return mGroupaction;
  }

  /**
   * Get other action
   * @return the other {@link FsAction}
   */
  public FsAction getOtherAction() {
    return mOtheraction;
  }

  private void set(FsAction u, FsAction g, FsAction o) {
    mUseraction = u;
    mGroupaction = g;
    mOtheraction = o;
  }

  public void fromShort(short n) {
    FsAction[] v = FsAction.values();
    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7]);
  }

  /**
   * Encodes the object to a short.
   */
  public short toShort() {
    int s = (mUseraction.ordinal() << 6) | (mGroupaction.ordinal() << 3)
        | mOtheraction.ordinal();
    return (short)s;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FsPermission) {
      FsPermission that = (FsPermission)obj;
      return mUseraction == that.mUseraction
          && mGroupaction == that.mGroupaction
          && mOtheraction == that.mOtheraction;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toShort();
  }

  @Override
  public String toString() {
    return mUseraction.getSymbol() + mGroupaction.getSymbol()
        + mOtheraction.getSymbol();
  }

  /**
   * Applies a umask to this permission and return a new one.
   *
   * @param umask the umask {@link FsPermission}
   * @return a new {@link FsPermission}
   */
  public FsPermission applyUMask(FsPermission umask) {
    return new FsPermission(mUseraction.and(umask.mUseraction.not()),
        mGroupaction.and(umask.mGroupaction.not()),
        mOtheraction.and(umask.mOtheraction.not()));
  }

  /**
   * Applies a umask to this permission and return a new one.
   *
   * @param conf the configuration to read the umask permission from
   * @return a new {@link FsPermission}
   */
  public FsPermission applyUMask(TachyonConf conf) {
    return applyUMask(getUMask(conf));
  }

  /**
   * Get the default permission.
   * @return the default {@link FsPermission}
   */
  public static FsPermission getDefault() {
    return new FsPermission(Constants.DEFAULT_TFS_FULL_PERMISSION);
  }

  /**
   * Get the none permission for NOSASL authentication mode
   * @return the none {@link FsPermission}
   */
  public static FsPermission getNoneFsPermission() {
    return new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);
  }

  /**
   * Get the file/directory creation umask
   * @return the umask {@link FsPermission}
   */
  public static FsPermission getUMask(TachyonConf conf) {
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
        for (int i = 0; i <= lastIndex; i ++) {
          newUmask += (confUmask.charAt(i) - '0') << 3 * (lastIndex - i);
        }
        umask = newUmask;
      }
    }
    return new FsPermission((short)umask);
  }

  private static boolean tryParseInt(String value) {
    try {
      Integer.parseInt(value);
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }
}
