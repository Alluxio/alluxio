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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

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
@NotThreadSafe
@JsonSerialize(using = AccessControlList.AccessControlListSerializer.class)
@JsonDeserialize(using = AccessControlList.AccessControlListDeserializer.class)
public class AccessControlList implements Serializable {
  private static final long serialVersionUID = 106023217076996L;

  public static final AccessControlList EMPTY_ACL = new AccessControlList();

  /** Keys representing owning user and group for proto ser/de. */
  public static final String OWNER_FIELD = "owner";
  public static final String OWNING_GROUP_FIELD = "owningGroup";
  public static final String STRING_ENTRY_FIELD = "stringEntries";

  public static final String OWNING_USER_KEY = "";
  public static final String OWNING_GROUP_KEY = "";

  protected String mOwningUser;
  protected String mOwningGroup;
  protected short mMode;
  protected ExtendedACLEntries mExtendedEntries;

  /**
   * Creates a new instance where owning user and owning group are initialized to empty strings,
   * and no action is permitted for any user or group.
   */
  public AccessControlList() {
    mOwningUser = "";
    mOwningGroup = "";
    clearEntries();
  }

  /**
   * Clears out all entries (does not modify the owner name and owning group).
   */
  public void clearEntries() {
    mMode = 0;
    mExtendedEntries = null;
  }

  /**
   * @return the extended entries
   */
  public ExtendedACLEntries getExtendedEntries() {
    return mExtendedEntries;
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

  /**
   * @return the owning user actions
   */
  public AclActions getOwningUserActions() {
    return Mode.extractOwnerBits(mMode).toAclActions();
  }

  /**
   * @return the owning group actions
   */
  public AclActions getOwningGroupActions() {
    return Mode.extractGroupBits(mMode).toAclActions();
  }

  /**
   * @return other actions
   */
  public AclActions getOtherActions() {
    return Mode.extractOtherBits(mMode).toAclActions();
  }

  /**
   * @return the permission mode defined in {@link Mode} for owning user, owning group, and other
   */
  public short getMode() {
    return mMode;
  }

  /**
   * Returns a list of {@link AclEntry} which represent this ACL instance. The mask will only be
   * included if extended ACL entries exist.
   *
   * @return an immutable list of ACL entries
   */
  public List<AclEntry> getEntries() {
    ImmutableList.Builder<AclEntry> builder = new ImmutableList.Builder<>();
    builder.add(new AclEntry.Builder()
        .setType(AclEntryType.OWNING_USER)
        .setSubject(mOwningUser)
        .setActions(getOwningUserActions())
        .build());
    builder.add(new AclEntry.Builder()
        .setType(AclEntryType.OWNING_GROUP)
        .setSubject(mOwningGroup)
        .setActions(getOwningGroupActions())
        .build());
    builder.add(new AclEntry.Builder()
        .setType(AclEntryType.OTHER)
        .setActions(getOtherActions())
        .build());
    if (hasExtended()) {
      builder.addAll(mExtendedEntries.getEntries());
    }
    return builder.build();
  }

  /**
   * @return true if has extended ACL (named users, named groups)
   */
  public boolean hasExtended() {
    return mExtendedEntries != null && mExtendedEntries.hasExtended();
  }

  /**
   * Removes the specified entry. A base entry is not allowed to be removed.
   *
   * @param entry the entry to be removed
   */
  public void removeEntry(AclEntry entry) {
    switch (entry.getType()) {
      case NAMED_USER:  // fall through
      case NAMED_GROUP: // fall through
      case MASK:
        if (mExtendedEntries != null) {
          mExtendedEntries.removeEntry(entry);
        }
        return;
      case OWNING_USER:  // fall through
      case OWNING_GROUP: // fall through
      case OTHER:
        throw new IllegalStateException(
            "Deleting base entry is not allowed. entry: " + entry);
      default:
        throw new IllegalStateException("Unknown ACL entry type: " + entry.getType());
    }
  }

  /**
   * Removes all of the exnteded entries. The base entries are retained.
   */
  public void removeExtendedEntries() {
    mExtendedEntries = null;
  }

  /**
   * Sets owning user.
   *
   * @param user the owning user
   */
  public void setOwningUser(String user) {
    Preconditions.checkNotNull(user);
    mOwningUser = user;
  }

  /**
   * Sets owning group.
   *
   * @param group the owning group
   */
  public void setOwningGroup(String group) {
    Preconditions.checkNotNull(group);
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
    mMode = mode;
  }

  /**
   * Update the mask to be the union of owning group entry, named user entry and named group entry.
   * This method must be called when the aforementioned entries are modified.
   */
  public void updateMask() {
    if (hasExtended()) {
      AclActions actions = getOwningGroupActions();
      mExtendedEntries.updateMask(actions);
    }
  }

  /**
   * Sets an entry into the access control list.
   * If an entry with the same type and subject already exists, overwrites the existing entry;
   * Otherwise, adds this new entry.
   * After we modify entries for NAMED_GROUP, OWNING_GROUP, NAMED_USER, we need to update the mask.
   *
   * @param entry the entry to be added or updated
   */
  public void setEntry(AclEntry entry) {
    switch (entry.getType()) {
      case NAMED_USER:  // fall through
      case NAMED_GROUP: // fall through
      case MASK:
        if (mExtendedEntries == null) {
          mExtendedEntries = new ExtendedACLEntries();
        }
        mExtendedEntries.setEntry(entry);
        return;
      case OWNING_USER:
        Mode modeOwner = new Mode(mMode);
        modeOwner.setOwnerBits(entry.getActions().toModeBits());
        mMode = modeOwner.toShort();
        return;
      case OWNING_GROUP:
        Mode modeGroup = new Mode(mMode);
        modeGroup.setGroupBits(entry.getActions().toModeBits());
        mMode = modeGroup.toShort();
        return;
      case OTHER:
        Mode modeOther = new Mode(mMode);
        modeOther.setOtherBits(entry.getActions().toModeBits());
        mMode = modeOther.toShort();
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
   * 2. Else if the user matches the name of one of the named user entries, then return the AND
   *    result of the permission in this entry and the mask ;
   * 3. Else if at least one of the groups is the owning group or matches the name of one of the
   *    named group entries, then for the named group entries that match a member of groups, merge
   *    the permissions in these entries and return the merged permission ANDed with the mask;
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
    if (hasExtended()) {
      AclActions actions = mExtendedEntries.getNamedUser(user);
      if (actions != null) {
        AclActions result = new AclActions(actions);
        result.mask(mExtendedEntries.mMaskActions);
        return result;
      }
    }

    boolean isGroupKnown = false;
    AclActions groupActions = new AclActions();
    if (groups.contains(mOwningGroup)) {
      isGroupKnown = true;
      groupActions.merge(getOwningGroupActions());
    }
    if (hasExtended()) {
      for (String group : groups) {
        AclActions actions = mExtendedEntries.getNamedGroup(group);
        if (actions != null) {
          isGroupKnown = true;
          groupActions.merge(actions);
        }
      }
    }
    if (isGroupKnown) {
      if (hasExtended()) {
        groupActions.mask(mExtendedEntries.mMaskActions);
      }
      return groupActions;
    }

    return getOtherActions();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccessControlList that = (AccessControlList) o;

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
        && extendedEquals;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOwningUser, mOwningGroup, mMode, mExtendedEntries);
  }

  /**
   * Converts a list of string entries into an AccessControlList or a DefaultAccessControlList.
   * It assumes the stringEntries contain all default entries or normal entries.
   *
   * @param owner the owner
   * @param owningGroup the owning group
   * @param stringEntries the list of string representations of the entries
   * @return the {@link AccessControlList} instance
   */
  public static AccessControlList fromStringEntries(String owner, String owningGroup,
      List<String> stringEntries) {
    AccessControlList acl;
    if (stringEntries.size() > 0) {
      AclEntry aclEntry = AclEntry.fromCliString(stringEntries.get(0));
      if (aclEntry.isDefault()) {
        acl = new DefaultAccessControlList();
      } else {
        acl = new AccessControlList();
      }
    } else {
      // when the stringEntries size is 0, it can only be a DefaultAccessControlList
      acl = new DefaultAccessControlList();
    }
    acl.setOwningUser(owner);
    acl.setOwningGroup(owningGroup);

    for (String stringEntry : stringEntries) {
      AclEntry aclEntry = AclEntry.fromCliString(stringEntry);
      acl.setEntry(aclEntry);
    }
    return acl;
  }

  /**
   * @return the list of string entries
   */
  public List<String> toStringEntries() {
    List<String> entries = new ArrayList<>();
    for (AclEntry entry : getEntries()) {
      entries.add(entry.toCliString());
    }
    return entries;
  }

  @Override
  public String toString() {
    List<String> entries = toStringEntries();
    return String.join(",", entries);
  }

  /**
   * This is a custom json serializer for AccessControlList class.
   */
  public static class AccessControlListSerializer extends StdSerializer<AccessControlList> {
    private static final long serialVersionUID = -8523910728069876504L;

    /**
     * Creates a AccessControlListSerializer.
     */
    public AccessControlListSerializer() {
      super(AccessControlList.class);
    }

    /**
     * Serialize an AccessControlList object.
     * @param accessControlList the ACL object to be serialized
     * @param jsonGenerator json generator
     * @param serializerProvider default serializer
     * @throws IOException
     */
    @Override
    public void serialize(AccessControlList accessControlList, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField(OWNER_FIELD, accessControlList.getOwningUser());
      jsonGenerator.writeStringField(OWNING_GROUP_FIELD, accessControlList.getOwningGroup());
      jsonGenerator.writeObjectField(STRING_ENTRY_FIELD, accessControlList.toStringEntries());
      jsonGenerator.writeEndObject();
    }
  }

  /**
   * This is a custom json deserializer for AccessControlList class.
   */
  public static class AccessControlListDeserializer extends StdDeserializer<AccessControlList> {
    private static final long serialVersionUID = 5524283318028333563L;

    /**
     * Creates a AccessControlListDeserializer.
     */
    public AccessControlListDeserializer() {
      super(AccessControlList.class);
    }

    /**
     * Deserialize an AccessControlList object.
     * @param jsonParser the json parser
     * @param deserializationContext deserializationcontext
     * @return
     * @throws IOException
     * @throws JsonProcessingException
     */
    @Override
    public AccessControlList deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
      JsonNode node = jsonParser.getCodec().readTree(jsonParser);
      String owner = node.get(OWNER_FIELD).asText();
      String owningGroup = node.get(OWNING_GROUP_FIELD).asText();
      List<String> stringEntries = new ArrayList<>();
      Iterator<JsonNode> nodeIterator = node.get(STRING_ENTRY_FIELD).elements();
      while (nodeIterator.hasNext()) {
        stringEntries.add(nodeIterator.next().asText());
      }
      return AccessControlList.fromStringEntries(owner, owningGroup, stringEntries);
    }
  }
}
