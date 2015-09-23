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

/**
 * A class for file/directory permissions.
 */
public class FsPermission {
  //POSIX permission style
  private FsAction mUseraction = null;
  private FsAction mGroupaction = null;
  private FsAction mOtheraction = null;

  /**
   * Construct by the given {@link FsAction}.
   * @param u user action
   * @param g group action
   * @param o other action
   */
  public FsPermission(FsAction u, FsAction g, FsAction o) {
    set(u, g, o);
  }

  /**
   * Construct by the given mode.
   * @param mode
   * @see #toShort()
   */
  public FsPermission(short mode) {
    fromShort(mode);
  }

  /**
   * Copy constructor
   *
   * @param otherPermission
   */
  public FsPermission(FsPermission otherPermission) {
    mUseraction = otherPermission.mUseraction;
    mGroupaction = otherPermission.mGroupaction;
    mOtheraction = otherPermission.mOtheraction;
  }

  /** Return user {@link FsAction}. */
  public FsAction getUserAction() {
    return mUseraction;
  }

  /** Return group {@link FsAction}. */
  public FsAction getGroupAction() {
    return mGroupaction;
  }

  /** Return other {@link FsAction}. */
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
   * Encode the object to a short.
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
   * Apply a umask to this permission and return a new one
   *
   * @param umask the umask {@code FsPermission}
   * @return a new FsPermission
   */
  public FsPermission applyUMask(FsPermission umask) {
    return new FsPermission(mUseraction.and(umask.mUseraction.not()),
        mGroupaction.and(umask.mGroupaction.not()),
        mOtheraction.and(umask.mOtheraction.not()));
  }

  /**
   * Apply a umask to this permission and return a new one
   *
   * @param conf Get the umask permission from the configuration
   * @return a new FsPermission
   */
  public FsPermission applyUMask(TachyonConf conf) {
    return applyUMask(getUMask(conf));
  }

  /** Get the default permission. */
  public static FsPermission getDefault() {
    return new FsPermission(Constants.DEFAULT_TFS_FULL_PERMISSION);
  }

  /**
   * Get the file/directory creation umask
   *
   */
  public static FsPermission getUMask(TachyonConf conf) {
    int umask = Constants.DEFAULT_TFS_PERMISSIONS_UMASK;
    if (conf != null) {
      umask = conf.getInt(Constants.TFS_PERMISSIONS_UMASK_KEY);
    }
    return new FsPermission((short)umask);
  }
}
