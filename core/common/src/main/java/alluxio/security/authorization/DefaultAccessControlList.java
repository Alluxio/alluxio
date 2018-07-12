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

import com.google.common.base.Objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Default Access control list for a directory.
 */
public class DefaultAccessControlList extends AccessControlList {
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
    mEmpty = false;
    mAccessAcl = acl;
    mOwningUser = acl.mOwningUser;
    mOwningGroup = acl.mOwningGroup;
    mUserActions.put(OWNING_USER_KEY, new AclActions(acl.mUserActions.get(OWNING_USER_KEY)));
    mGroupActions.put(OWNING_GROUP_KEY, new AclActions(acl.mGroupActions.get(OWNING_GROUP_KEY)));
    mOtherActions = new AclActions(acl.mOtherActions);
  }

  /**
   * create a child file 's accessACL based on the default ACL.
   * @return child file's access ACL
   */
  public AccessControlList generateChildFileACL() {
    AccessControlList acl = new AccessControlList();
    acl.mOwningUser = mOwningUser;
    acl.mOwningGroup = mOwningGroup;
    acl.mUserActions = new HashMap<>(mUserActions);
    acl.mGroupActions = new HashMap<>(mGroupActions);
    acl.mOtherActions = new AclActions(mOtherActions);
    acl.mMaskActions = new AclActions(mMaskActions);
    return acl;
  }

  /**
   * Creates a child directory's access ACL and default ACL based on the default ACL.
   * @return child directory's access ACL and default ACL
   */
  public Pair<AccessControlList, DefaultAccessControlList> generateChildDirACL() {
    AccessControlList acl = generateChildFileACL();
    DefaultAccessControlList dAcl = new DefaultAccessControlList(acl);
    dAcl.mOwningUser = mOwningUser;
    dAcl.mOwningGroup = mOwningGroup;
    dAcl.mUserActions = new HashMap<>(mUserActions);
    dAcl.mGroupActions = new HashMap<>(mGroupActions);
    dAcl.mOtherActions = new AclActions(mOtherActions);
    dAcl.mMaskActions = new AclActions(mMaskActions);
    return new Pair<>(acl, dAcl);
  }

  private void fillDefault() {
    if (mAccessAcl == null) {
      return;
    }
    setEmpty(false);
    if (mUserActions.get(OWNING_USER_KEY) == null) {
      mUserActions.put(OWNING_USER_KEY,
          new AclActions(mAccessAcl.mUserActions.get(OWNING_USER_KEY)));
    }
    if (mGroupActions.get(OWNING_GROUP_KEY) == null) {
      mGroupActions.put(OWNING_GROUP_KEY,
          new AclActions(mAccessAcl.mGroupActions.get(OWNING_GROUP_KEY)));
    }
    if (mOtherActions == null) {
      mOtherActions = new AclActions(mAccessAcl.mOtherActions);
    }
  }

  /**
   * Removes the specified entry. A base entry is not allowed to be removed.
   *
   * @param entry the entry to be removed
   */
  @Override
  public void removeEntry(AclEntry entry) throws IOException {
    switch (entry.getType()) {
      case NAMED_USER:
      case NAMED_GROUP:
      case MASK:
        super.removeEntry(entry);
        return;
      case OWNING_USER:
        mUserActions.remove(entry.getSubject());
        fillDefault();
        return;
      case OWNING_GROUP:
        mGroupActions.remove(entry.getSubject());
        fillDefault();
        return;
      case OTHER:
        mOtherActions = null;
        fillDefault();
        return;
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  @Override
  public void setEntry(AclEntry entry) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultAccessControlList)) {
      return false;
    }
    DefaultAccessControlList that = (DefaultAccessControlList) o;
    return mOwningUser.equals(that.mOwningUser)
        && mOwningGroup.equals(that.mOwningGroup)
        && mUserActions.equals(that.mUserActions)
        && mGroupActions.equals(that.mGroupActions)
        && mMaskActions.equals(that.mMaskActions)
        && mOtherActions.equals(that.mOtherActions)
        && mEmpty == (that.mEmpty);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOwningUser, mOwningGroup, mUserActions, mGroupActions, mMaskActions,
        mOtherActions, mEmpty);
  }
}
