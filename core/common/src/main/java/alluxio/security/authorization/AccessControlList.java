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

import alluxio.proto.journal.File;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Access control list for a file or directory.
 */
@NotThreadSafe
public final class AccessControlList {
  /**
   * Initial capacity for {@link #mUserActions} and {@link #mGroupActions}.
   * Most of the time, only owning user and owning group exists in {@link #mUserActions} and
   * {@link #mGroupActions}.
   */
  private static final int ACTIONS_MAP_INITIAL_CAPACITY = 1;
  /** Initial load factor. */
  private static final int ACTIONS_MAP_INITIAL_LOAD_FACTOR = 1;
  /** Key representing owning user in {@link #mUserActions}. */
  private static final String OWNING_USER_KEY = "";
  /** Key representing owning group in {@link #mGroupActions}. */
  private static final String OWNING_GROUP_KEY = "";

  private String mOwningUser;
  private String mOwningGroup;
  private Map<String, AclActions> mUserActions;
  private Map<String, AclActions> mGroupActions;
  private AclActions mMaskActions;
  private AclActions mOtherActions;

  /**
   * Creates a new instance where owning user and owning group are initialized to empty strings,
   * and no action is permitted for any user or group.
   */
  public AccessControlList() {
    mOwningUser = "";
    mOwningGroup = "";
    mUserActions = new HashMap<>(ACTIONS_MAP_INITIAL_CAPACITY, ACTIONS_MAP_INITIAL_LOAD_FACTOR);
    mUserActions.put(OWNING_USER_KEY, new AclActions());
    mGroupActions = new HashMap<>(ACTIONS_MAP_INITIAL_CAPACITY, ACTIONS_MAP_INITIAL_LOAD_FACTOR);
    mGroupActions.put(OWNING_GROUP_KEY, new AclActions());
    mMaskActions = new AclActions();
    mOtherActions = new AclActions();
  }

  /**
   * @return the owning user
   */
  public String getOwningUser() {
    return mOwningUser;
  }

  /**
   * @return the owning group
   */
  public String getOwningGroup() {
    return mOwningGroup;
  }

  private AclActions getOwningUserActions() {
    return mUserActions.get(OWNING_USER_KEY);
  }

  private AclActions getOwningGroupActions() {
    return mGroupActions.get(OWNING_GROUP_KEY);
  }

  /**
   * @return the permission mode defined in {@link Mode} for owning user, owning group, and other
   */
  public short getMode() {
    Mode.Bits owner = getOwningUserActions().toModeBits();
    Mode.Bits group = getOwningGroupActions().toModeBits();
    Mode.Bits other = mOtherActions.toModeBits();
    return new Mode(owner, group, other).toShort();
  }

  /**
   * Sets owning user.
   *
   * @param user the owning user
   */
  public void setOwningUser(String user) {
    mOwningUser = user;
  }

  /**
   * Sets owning group.
   *
   * @param group the owning group
   */
  public void setOwningGroup(String group) {
    mOwningGroup = group;
  }

  /**
   * Sets permitted actions for owning user, owning group, and other based on the mode.
   * The format of mode is defined in {@link Mode}.
   * The update logic is defined in {@link AclActions#updateByModeBits(Mode.Bits)}.
   *
   * @param mode the mode
   */
  public void setMode(short mode) {
    getOwningUserActions().updateByModeBits(Mode.extractOwnerBits(mode));
    getOwningGroupActions().updateByModeBits(Mode.extractGroupBits(mode));
    mOtherActions.updateByModeBits(Mode.extractOtherBits(mode));
  }

  /**
   * Sets an entry into the access control list.
   * If an entry with the same type and subject already exists, overwrites the existing entry;
   * Otherwise, adds this new entry.
   *
   * @param entry the entry to be added or updated
   */
  public void setEntry(AclEntry entry) {
    // TODO(cc): when setting non-mask entries, the mask should be dynamically updated too.
    switch (entry.getType()) {
      case OWNING_USER:
        setOwningUserEntry(entry);
        return;
      case NAMED_USER:
        setNamedUserEntry(entry);
        return;
      case OWNING_GROUP:
        setOwningGroupEntry(entry);
        return;
      case NAMED_GROUP:
        setNamedGroupEntry(entry);
        return;
      case MASK:
        setMaskEntry(entry);
        return;
      case OTHER:
        setOtherEntry(entry);
        return;
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  /**
   * Checks whether the user has the permission to perform the action.
   *
   * 1. If the user is the owner, then the owner entry determines the permission;
   * 2. Else if the user matches the name of one of the named user entries, this entry determines
   *    the permission;
   * 3. Else if one of the groups is the owning group and the owning group entry contains the
   *    requested permission, the permission is granted;
   * 4. Else if one of the groups matches the name of one of the named group entries and this entry
   *    contains the requested permission, the permission is granted;
   * 5. Else if one of the groups is the owning group or matches the name of one of the named group
   *    entries, but neither the owning group entry nor any of the matching named group entries
   *    contains the requested permission, the permission is denied;
   * 6. Otherwise, the other entry determines the permission.
   *
   * @param user the user
   * @param groups the groups the user belongs to
   * @param action the action
   * @return whether user has the permission to perform the action
   */
  public boolean checkPermission(String user, List<String> groups, AclAction action) {
    return getPermission(user, groups).contains(action);
  }

  /**
   * Gets the permitted actions for a user.
   *
   * When AccessControlList is not modified after calling getPermission,
   * for each action returned by this method, checkPermission(user, groups, action) is true,
   * for other actions, checkPermission(user, groups, action) is false.
   *
   * 1. If the user is the owner, then return the permission in the owner entry;
   * 2. Else if the user matches the name of one of the named user entries, then return the
   *    permission in this entry;
   * 3. Else if at least one of the groups is the owning group or matches the name of one of the
   *    named group entries, then for the named group entries that match a member of groups, merge
   *    the permissions in these entries and return the merged permission;
   * 4. Otherwise, return the permission in the other entry.
   *
   * @param user the user
   * @param groups the groups the user belongs to
   * @return the permitted actions
   */
  public AclActions getPermission(String user, List<String> groups) {
    if (user.equals(mOwningUser)) {
      return new AclActions(getOwningUserActions());
    }
    if (mUserActions.containsKey(user)) {
      return new AclActions(mUserActions.get(user));
    }

    boolean isGroupKnown = false;
    AclActions groupActions = new AclActions();
    if (groups.contains(mOwningGroup)) {
      isGroupKnown = true;
      groupActions.merge(getOwningGroupActions());
    }
    for (String group : groups) {
      if (mGroupActions.containsKey(group)) {
        isGroupKnown = true;
        groupActions.merge(mGroupActions.get(group));
      }
    }
    if (isGroupKnown) {
      return groupActions;
    }

    return new AclActions(mOtherActions);
  }

  private void setOwningUserEntry(AclEntry entry) {
    setOwningUser(entry.getSubject());
    mUserActions.put(OWNING_USER_KEY, entry.getActions());
  }

  private void setNamedUserEntry(AclEntry entry) {
    mUserActions.put(entry.getSubject(), entry.getActions());
  }

  private void setOwningGroupEntry(AclEntry entry) {
    setOwningGroup(entry.getSubject());
    mGroupActions.put(OWNING_GROUP_KEY, entry.getActions());
  }

  private void setNamedGroupEntry(AclEntry entry) {
    mGroupActions.put(entry.getSubject(), entry.getActions());
  }

  private void setMaskEntry(AclEntry entry) {
    mMaskActions = entry.getActions();
  }

  private void setOtherEntry(AclEntry entry) {
    mOtherActions = entry.getActions();
  }

  /**
   * @param acl the protobuf representation
   * @return {@link AccessControlList}
   */
  public static AccessControlList fromProtoBuf(File.AccessControlList acl) {
    AccessControlList ret = new AccessControlList();

    for (File.NamedAclActions namedActions : acl.getUserActionsList()) {
      String name = namedActions.getName();
      AclActions actions = AclActions.fromProtoBuf(namedActions.getActions());
      AclEntry entry;
      if (name.equals(OWNING_USER_KEY)) {
        entry = new AclEntry.Builder().setType(AclEntryType.OWNING_USER)
            .setSubject(acl.getOwningUser()).setActions(actions).build();
      } else {
        entry = new AclEntry.Builder().setType(AclEntryType.NAMED_USER)
            .setSubject(name).setActions(actions).build();
      }
      ret.setEntry(entry);
    }

    for (File.NamedAclActions namedActions : acl.getGroupActionsList()) {
      String name = namedActions.getName();
      AclActions actions = AclActions.fromProtoBuf(namedActions.getActions());
      AclEntry entry;
      if (name.equals(OWNING_GROUP_KEY)) {
        entry = new AclEntry.Builder().setType(AclEntryType.OWNING_GROUP)
            .setSubject(acl.getOwningGroup()).setActions(actions).build();
      } else {
        entry = new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP)
            .setSubject(name).setActions(actions).build();
      }
      ret.setEntry(entry);
    }

    AclActions actions = AclActions.fromProtoBuf(acl.getMaskActions());
    AclEntry entry = new AclEntry.Builder().setType(AclEntryType.MASK)
        .setActions(actions).build();
    ret.setEntry(entry);

    actions = AclActions.fromProtoBuf(acl.getOtherActions());
    entry = new AclEntry.Builder().setType(AclEntryType.OTHER)
        .setActions(actions).build();
    ret.setEntry(entry);

    return ret;
  }

  /**
   * @param acl {@link AccessControlList}
   * @return protobuf representation
   */
  public static File.AccessControlList toProtoBuf(AccessControlList acl) {
    File.AccessControlList.Builder builder = File.AccessControlList.newBuilder();
    builder.setOwningUser(acl.mOwningUser);
    builder.setOwningGroup(acl.mOwningGroup);
    builder.setMaskActions(AclActions.toProtoBuf(acl.mMaskActions));
    builder.setOtherActions(AclActions.toProtoBuf(acl.mOtherActions));
    for (Map.Entry<String, AclActions> kv : acl.mUserActions.entrySet()) {
      File.NamedAclActions namedActions = File.NamedAclActions.newBuilder()
          .setName(kv.getKey())
          .setActions(AclActions.toProtoBuf(kv.getValue()))
          .build();
      builder.addUserActions(namedActions);
    }
    for (Map.Entry<String, AclActions> kv : acl.mGroupActions.entrySet()) {
      File.NamedAclActions namedActions = File.NamedAclActions.newBuilder()
          .setName(kv.getKey())
          .setActions(AclActions.toProtoBuf(kv.getValue()))
          .build();
      builder.addGroupActions(namedActions);
    }
    return builder.build();
  }
}
