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

package tachyon.master.permission;

import com.google.common.base.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import tachyon.conf.TachyonConf;
import tachyon.master.permission.AclEntry.AclPermission;
import tachyon.master.permission.AclEntry.AclType;

/**
 * This class contains three {@link AclEntry}: mUserEntry, mGroupEntry, and mOtherEntry.
 */
public class Acl {

  //POSIX aclEntry
  private AclEntry mUserEntry;
  private AclEntry mGroupEntry;
  private AclEntry mOtherEntry;

  private static final Log LOG = LogFactory.getLog(Acl.class);

  /**
   * Construct by the given {@link AclEntry}.
   * @param u, user aclEntry
   * @param g, group aclEntry
   * @param o, other aclEntry
   */
  private Acl(AclEntry u, AclEntry g, AclEntry o) {
    this.mUserEntry = u;
    this.mGroupEntry = g;
    this.mOtherEntry = o;
  }

  /**
   * Copy constructor
   *
   * @param other, other permission
   */
  public Acl(Acl other) {
    this.mUserEntry = other.mUserEntry;
    this.mGroupEntry = other.mGroupEntry;
    this.mOtherEntry = other.mOtherEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    Acl other = (Acl)o;
    return Objects.equal(mUserEntry, other.mUserEntry)
        && Objects.equal(mGroupEntry, other.mGroupEntry)
        && Objects.equal(mOtherEntry, other.mOtherEntry);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUserEntry, mGroupEntry, mOtherEntry);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    if (mUserEntry != null) {
      sb.append('[')
        .append(mUserEntry.toString())
        .append(']');
    }
    sb.append(',');
    if (mGroupEntry != null) {
      sb.append('[')
        .append(mGroupEntry.toString())
        .append(']');
    }
    sb.append(',');
    if (mOtherEntry != null) {
      sb.append('[')
        .append(mOtherEntry.toString())
        .append(']');
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Retrun short of this ACL permission
   */
  public short toShort() {
    int s = (getUserPermission().ordinal() << 6)
        | (getGroupPermission().ordinal() << 3)
        | getOtherPermission().ordinal();
    return (short)s;
  }

  /**
   * Get a string permission format
   * @return string .ex rwxr-x--x
   */
  public String getPermission() {
    return mUserEntry.getPermission().mValue + mGroupEntry.getPermission().mValue
        + mOtherEntry.getPermission().mValue;
  }

  public AclPermission getUserPermission() {
    return mUserEntry.getPermission();
  }

  public AclPermission getGroupPermission() {
    return mGroupEntry.getPermission();
  }

  public AclPermission getOtherPermission() {
    return mOtherEntry.getPermission();
  }

  public String getUserName() {
    return mUserEntry.getName();
  }

  public String getGroupName() {
    return mGroupEntry.getName();
  }

  public void umask(Acl umask) {
    getUserPermission().and(umask.getUserPermission().not());
    getGroupPermission().and(umask.getGroupPermission().not());
    getOtherPermission().and(umask.getOtherPermission().not());
  }

  /**
   * Umask from a conf, umask should set to conf with key "tfs.permission.umask"
   *
   * @param conf
   */
  public void umask(TachyonConf conf) {
    umask(AclUtil.getUMask(conf));
  }

  /**
   * Umask from a short, e.g. umask 0002
   *
   * @param n, a short of umask
   */
  public void umask(short n) {
    AclPermission userUmask = AclUtil.toUserPermission(n);
    AclPermission groupUmask = AclUtil.toGroupPermission(n);
    AclPermission otherUmask = AclUtil.toOtherPermission(n);
    AclPermission newUserPermission = getUserPermission().and(userUmask.not());
    AclPermission newGroupPermission = getGroupPermission().and(groupUmask.not());
    AclPermission newOtherPermission = getOtherPermission().and(otherUmask.not());
    setPermission(newUserPermission, newGroupPermission, newOtherPermission);
  }

  /**
   * Set permission to AclEntry from a short
   * Used for chmod, e.g. chmod 777
   *
   * @param n, a short of permission
   */
  public void setPermission(short n) {
    setPermission(
        AclUtil.toUserPermission(n),
        AclUtil.toGroupPermission(n),
        AclUtil.toOtherPermission(n));
  }

  private void setPermission(AclPermission u, AclPermission g, AclPermission o) {
    mUserEntry.setPermission(u);
    mGroupEntry.setPermission(g);
    mOtherEntry.setPermission(o);
  }

  /**
   * Set user's owner from a given name
   * Used for chown
   *
   * @param name, new owner name
   */
  public void setUserOwner(String name) {
    mUserEntry.setName(name);
  }

  /**
   * Set group's owner from a given name
   * Used for chown
   *
   * @param name, new owner name
   */
  public void setGroupOwner(String name) {
    mGroupEntry.setName(name);
  }

  /**
   * Builder for creating new ACL instances.
   */
  public static class Builder {
    private AclEntry mUserEntry;
    private AclEntry mGroupEntry;
    private AclEntry mOtherEntry;

    public Builder setUserEntry(AclEntry u) {
      this.mUserEntry = u;
      return this;
    }

    public Builder setGroupEntry(AclEntry g) {
      this.mGroupEntry = g;
      return this;
    }

    public Builder setOtherEntry(AclEntry o) {
      this.mOtherEntry = o;
      return this;
    }

    public Acl build() {
      return new Acl(mUserEntry, mGroupEntry, mOtherEntry);
    }
  }
}