/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.security.authorization;

import alluxio.collections.Pair;
import alluxio.thrift.TAcl;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

/**
 * Default Access control list for a directory.
 */
public class DefaultAccessControlList extends AccessControlList {
  private static final long serialVersionUID = 8649647787531425489L;

  public static final DefaultAccessControlList EMPTY_DEFAULT_ACL = new DefaultAccessControlList();

  /**
   * a reference to the access ACL associated with the same inode so that we can fill the default
   * value for OWNING_USER, OWNING_GROUP and OTHER.
   */
  private AccessControlList mAccessAcl;

  /**
   * mEmpty property is used to indicate whether the defaultACL is empty or not.
   * When it is not empty, it has at least entries for OWNING_USER, OWNING_GROUP and OTHER.
   */
  private boolean mEmpty;
  /**
   * Constructor to build a default ACL that is empty.
   */
  public DefaultAccessControlList() {
    mEmpty = true;
  }

  /**
   * Constructor to build a default ACL based on an access ACL.
   * @param acl the access acl
   */
  public DefaultAccessControlList(AccessControlList acl) {
    super();
    mEmpty = true;
    mAccessAcl = acl;
    mOwningUser = acl.mOwningUser;
    mOwningGroup = acl.mOwningGroup;
    mMode = acl.mMode;
  }

  /**
   * create a child file 's accessACL based on the default ACL.
   * @return child file's access ACL
   */
  public AccessControlList generateChildFileACL() {
    AccessControlList acl = new AccessControlList();
    acl.mOwningUser = mOwningUser;
    acl.mOwningGroup = mOwningGroup;
    acl.mMode = mMode;
    if (mExtendedEntries == null) {
      acl.mExtendedEntries = null;
    } else {
      acl.mExtendedEntries = new ExtendedACLEntries(mExtendedEntries);
    }
    return acl;
  }

  /**
   * Creates a child directory's access ACL and default ACL based on the default ACL.
   * @return child directory's access ACL and default ACL
   */
  public Pair<AccessControlList, DefaultAccessControlList> generateChildDirACL() {
    AccessControlList acl = generateChildFileACL();
    DefaultAccessControlList dAcl = new DefaultAccessControlList(acl);
    dAcl.setEmpty(false);
    dAcl.mOwningUser = mOwningUser;
    dAcl.mOwningGroup = mOwningGroup;
    dAcl.mMode = mMode;
    if (mExtendedEntries == null) {
      dAcl.mExtendedEntries = null;
    } else {
      dAcl.mExtendedEntries = new ExtendedACLEntries(mExtendedEntries);
    }
    return new Pair<>(acl, dAcl);
  }

  /**
   * Removes the specified entry. A base entry is not allowed to be removed.
   *
   * @param entry the entry to be removed
   */
  @Override
  public void removeEntry(AclEntry entry) {
    switch (entry.getType()) {
      case NAMED_USER:
      case NAMED_GROUP:
      case MASK:
        super.removeEntry(entry);
        return;
      case OWNING_USER:
        Mode modeOwner = new Mode(mMode);
        modeOwner.setOwnerBits(Mode.Bits.NONE);
        if (mAccessAcl != null) {
          // overwrite the owner actions from the access ACL.
          modeOwner.setOwnerBits(new Mode(mAccessAcl.mMode).getOwnerBits());
        }
        mMode = modeOwner.toShort();
        return;
      case OWNING_GROUP:
        Mode modeGroup = new Mode(mMode);
        modeGroup.setGroupBits(Mode.Bits.NONE);
        if (mAccessAcl != null) {
          // overwrite the group actions from the access ACL.
          modeGroup.setGroupBits(new Mode(mAccessAcl.mMode).getGroupBits());
        }
        mMode = modeGroup.toShort();
        return;
      case OTHER:
        Mode modeOther = new Mode(mMode);
        modeOther.setOtherBits(Mode.Bits.NONE);
        if (mAccessAcl != null) {
          // overwrite the other actions from the access ACL.
          modeOther.setOtherBits(new Mode(mAccessAcl.mMode).getOtherBits());
        }
        mMode = modeOther.toShort();
        return;
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  @Override
  public void setEntry(AclEntry entry) {
    if (isEmpty() && mAccessAcl != null) {
      mMode = mAccessAcl.mMode;
    }
    super.setEntry(entry);
    setEmpty(false);
  }

  /**
   * Returns true if the default ACL is empty.
   * @return true if empty
   */
  public boolean isEmpty() {
    return mEmpty;
  }

  /**
   * @param empty set the DefaultAccessControlList to empty or not
   */
  public void setEmpty(boolean empty) {
    mEmpty = empty;
  }

  /**
   * Returns a list of {@link AclEntry} which represent this ACL instance. The mask will only be
   * included if extended ACL entries exist.
   *
   * @return an immutable list of ACL entries
   */
  @Override
  public List<AclEntry> getEntries() {
    if (isEmpty()) {
      return new ArrayList<>();
    }
    List<AclEntry> aclEntryList = super.getEntries();

    for (AclEntry entry : aclEntryList) {
      entry.setDefault(true);
    }
    return aclEntryList;
  }

  /**
   * @return the thrift representation of this object
   */
  @Override
  public TAcl toThrift() {
    TAcl tAcl = super.toThrift();
    tAcl.setIsDefault(true);
    tAcl.setIsDefaultEmpty(isEmpty());
    return tAcl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultAccessControlList)) {
      return false;
    }
    DefaultAccessControlList that = (DefaultAccessControlList) o;
    // If the extended acl object is empty (does not have any extended entries), it is equivalent
    // to a null object.
    boolean extendedNull = (mExtendedEntries == null && that.mExtendedEntries == null);
    boolean extendedNotNull1 =
        mExtendedEntries != null && (mExtendedEntries.equals(that.mExtendedEntries) || (
            !mExtendedEntries.hasExtended() && that.mExtendedEntries == null));
    boolean extendedNotNull2 =
        that.mExtendedEntries != null && (that.mExtendedEntries.equals(mExtendedEntries) || (
            !that.mExtendedEntries.hasExtended() && mExtendedEntries == null));
    boolean extendedEquals = extendedNull || extendedNotNull1 || extendedNotNull2;

    return mOwningUser.equals(that.mOwningUser)
        && mOwningGroup.equals(that.mOwningGroup)
        && mMode == that.mMode
        && extendedEquals
        && mEmpty == (that.mEmpty);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOwningUser, mOwningGroup, mMode, mExtendedEntries, mEmpty);
  }
}
