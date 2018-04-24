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

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Access control list for a file or directory.
 *
 * An access control list is conceptually a list of entries, there are different types of entries:
 * 1. owning user entry which specifies permitted actions for the owning user of a file or
 *    directory, there is only one owning user entry;
 * 2. named user entry which specifies permitted actions for any user, there is only one named
 *    user entry for each user;
 * 3. owning group entry which specifies permitted actions for the owning group of a file or
 *    directory, there is only one owning group entry;
 * 4. named group entry which specifies permitted actions for any group, there is only one named
 *    group entry for each group;
 * 5. mask entry which specifies the maximum set of permitted actions for users and groups in all
 *    the above entries;
 * 6. other entry which specifies permitted actions for users who are neither the owning user nor
 *    have a named user entry, and whose belonging groups are neither the owning group nor have a
 *    named group entry.
 *
 * Also, the access control list contains owning user and owning group of a file or directory.
 */
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
   * @return an immutable list of ACL entries, if owning user or owning group is empty, no owning
   *    user entry or owning group entry will be returned, named user entry and named group entry
   *    will be returned if they exist, mask and other entry will always be returned
   */
  public List<AclEntry> getEntries() {
    ImmutableList.Builder<AclEntry> builder = new ImmutableList.Builder<>();
    for (Map.Entry<String, AclActions> kv : mUserActions.entrySet()) {
      if (kv.getKey().equals(OWNING_USER_KEY)) {
        if (!mOwningUser.isEmpty()) {
          builder.add(new AclEntry.Builder()
              .setType(AclEntryType.OWNING_USER)
              .setSubject(mOwningUser)
              .setActions(getOwningUserActions())
              .build());
        }
      } else {
        builder.add(new AclEntry.Builder()
            .setType(AclEntryType.NAMED_USER)
            .setSubject(kv.getKey())
            .setActions(kv.getValue())
            .build());
      }
    }
    for (Map.Entry<String, AclActions> kv : mGroupActions.entrySet()) {
      if (kv.getKey().equals(OWNING_GROUP_KEY)) {
        if (!mOwningGroup.isEmpty()) {
          builder.add(new AclEntry.Builder()
              .setType(AclEntryType.OWNING_GROUP)
              .setSubject(mOwningGroup)
              .setActions(getOwningGroupActions())
              .build());
        }
      } else {
        builder.add(new AclEntry.Builder()
            .setType(AclEntryType.NAMED_GROUP)
            .setSubject(kv.getKey())
            .setActions(kv.getValue())
            .build());
      }
    }
    builder.add(new AclEntry.Builder()
        .setType(AclEntryType.OTHER)
        .setActions(mOtherActions)
        .build());
    builder.add(new AclEntry.Builder()
        .setType(AclEntryType.MASK)
        .setActions(mMaskActions)
        .build());
    return builder.build();
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
   * @param user the user
   * @param groups the groups the user belongs to
   * @param action the action
   * @return whether user has the permission to perform the action
   */
  public boolean check(String user, List<String> groups, AclAction action) {
    // TODO(cc): Update the logic to take MASK into consideration.
    if (user.equals(mOwningUser)) {
      return getOwningUserActions().contains(action);
    }
    if (mUserActions.containsKey(user)) {
      return mUserActions.get(user).contains(action);
    }

    boolean isGroupKnown = false;
    if (groups.contains(mOwningGroup)) {
      isGroupKnown = true;
      if (getOwningGroupActions().contains(action)) {
        return true;
      }
    }
    for (String group : groups) {
      if (mGroupActions.containsKey(group)) {
        isGroupKnown = true;
        if (mGroupActions.get(group).contains(action)) {
          return true;
        }
      }
    }
    if (isGroupKnown) {
      return false;
    }

    return mOtherActions.contains(action);
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
