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

package alluxio.util.proto;

import alluxio.exception.status.Status;
import alluxio.proto.journal.File;
import alluxio.proto.status.Status.PStatus;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.wire.SetAclAction;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Protobuf related utils.
 */
public final class ProtoUtils {
  private ProtoUtils() {} // prevent instantiation

  /**
   * A wrapper of {@link CodedInputStream#readRawVarint32(InputStream)}.
   *
   * @param firstByte first byte in the input stream
   * @param input input stream
   * @return an int value read from the input stream
   */
  public static int readRawVarint32(int firstByte, InputStream input) throws IOException {
    return CodedInputStream.readRawVarint32(firstByte, input);
  }

  /**
   * Checks whether the exception is an {@link InvalidProtocolBufferException} thrown because of
   * a truncated message.
   *
   * @param e the exception
   * @return whether the exception is an {@link InvalidProtocolBufferException} thrown because of
   *         a truncated message.
   */
  public static boolean isTruncatedMessageException(IOException e) {
    if (!(e instanceof InvalidProtocolBufferException)) {
      return false;
    }
    String truncatedMessage;
    try {
      Method method = InvalidProtocolBufferException.class.getMethod("truncatedMessage");
      method.setAccessible(true);
      truncatedMessage = (String) method.invoke(null);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ee) {
      throw new RuntimeException(ee);
    }
    return e.getMessage().equals(truncatedMessage);
  }

  /**
   * @param acl {@link AccessControlList}
   * @return protobuf representation
   */
  public static File.AccessControlList toProto(AccessControlList acl) {
    File.AccessControlList.Builder builder = File.AccessControlList.newBuilder();
    builder.setOwningUser(acl.mOwningUser);
    builder.setOwningGroup(acl.mOwningGroup);

    // base entries
    builder.addUserActions(File.NamedAclActions.newBuilder()
        .setName(OWNING_USER_KEY)
        .setActions(AclActions.toProtoBuf(acl.getOwningUserActions()))
        .build());
    builder.addGroupActions(File.NamedAclActions.newBuilder()
        .setName(OWNING_GROUP_KEY)
        .setActions(AclActions.toProtoBuf(acl.getOwningGroupActions()))
        .build());
    builder.setOtherActions(AclActions.toProtoBuf(acl.getOtherActions()));

    if (acl.mExtendedEntries != null) {
      builder.addAllUserActions(acl.mExtendedEntries.getNamedUsersProto());
      builder.addAllGroupActions(acl.mExtendedEntries.getNamedGroupsProto());
      builder.setMaskActions(AclActions.toProtoBuf(acl.mExtendedEntries.getMask()));
    }

    if (acl instanceof DefaultAccessControlList) {
      DefaultAccessControlList defaultAcl = (DefaultAccessControlList) acl;
      builder.setIsDefault(true);
      builder.setIsEmpty(defaultAcl.isEmpty());
    } else {
      builder.setIsDefault(false);
      // non default acl is always not empty
      builder.setIsEmpty(false);
    }
    return builder.build();
  }

  /**
   * @return the protobuf representation of action
   */
  public static File.AclAction toProto(AclAction aclAction) {
    switch (aclAction) {
      case READ:
        return File.AclAction.READ;
      case WRITE:
        return File.AclAction.WRITE;
      case EXECUTE:
        return File.AclAction.EXECUTE;
      default:
        throw new IllegalStateException("Unknown acl action: " + aclAction);
    }
  }

  /**
   * @return the proto representation of instance
   */
  public static File.AclEntry toProto(AclEntry aclEntry) {
    File.AclEntry.Builder builder = File.AclEntry.newBuilder();
    builder.setType(toProto(aclEntry.getType()));
    builder.setSubject(aclEntry.getSubject());
    builder.setIsDefault(aclEntry.isDefault());
    for (AclAction action : aclEntry.getActions().getActions()) {
      builder.addActions(toProto(action));
    }
    return builder.build();
  }

  /**
   * Converts wire type to proto type.
   *
   * @return {@link TSetAclAction} equivalent
   */
  public static File.SetAclAction toProto(SetAclAction aclAction) {
    switch (aclAction) {
      case REPLACE:
        return File.SetAclAction.REPLACE;
      case MODIFY:
        return File.SetAclAction.MODIFY;
      case REMOVE:
        return File.SetAclAction.REMOVE;
      case REMOVE_ALL:
        return File.SetAclAction.REMOVE_ALL;
      case REMOVE_DEFAULT:
        return File.SetAclAction.REMOVE_DEFAULT;
      default:
        throw new IllegalStateException("Unrecognized set acl action: " + aclAction);
    }
  }
  
  /**
   * Converts an internal exception status to a protocol buffer type status.
   *
   * @param status the status to convert
   * @return the protocol buffer type status
   */
  public static PStatus toProto(Status status) {
    switch (status) {
      case ABORTED:
        return PStatus.ABORTED;
      case ALREADY_EXISTS:
        return PStatus.ALREADY_EXISTS;
      case CANCELED:
        return PStatus.CANCELED;
      case DATA_LOSS:
        return PStatus.DATA_LOSS;
      case DEADLINE_EXCEEDED:
        return PStatus.DEADLINE_EXCEEDED;
      case FAILED_PRECONDITION:
        return PStatus.FAILED_PRECONDITION;
      case INTERNAL:
        return PStatus.INTERNAL;
      case INVALID_ARGUMENT:
        return PStatus.INVALID_ARGUMENT;
      case NOT_FOUND:
        return PStatus.NOT_FOUND;
      case OK:
        return PStatus.OK;
      case OUT_OF_RANGE:
        return PStatus.OUT_OF_RANGE;
      case PERMISSION_DENIED:
        return PStatus.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return PStatus.RESOURCE_EXHAUSTED;
      case UNAUTHENTICATED:
        return PStatus.UNAUTHENTICATED;
      case UNAVAILABLE:
        return PStatus.UNAVAILABLE;
      case UNIMPLEMENTED:
        return PStatus.UNIMPLEMENTED;
      case UNKNOWN:
        return PStatus.UNKNOWN;
      default:
        return PStatus.UNKNOWN;
    }
  }

  /**
   * @return the proto representation of enum
   */
  public static File.AclEntryType toProto(AclEntryType entryType) {
    switch (entryType) {
      case OWNING_USER:
        return File.AclEntryType.OWNER;
      case NAMED_USER:
        return File.AclEntryType.NAMED_USER;
      case OWNING_GROUP:
        return File.AclEntryType.OWNING_GROUP;
      case NAMED_GROUP:
        return File.AclEntryType.NAMED_GROUP;
      case MASK:
        return File.AclEntryType.MASK;
      case OTHER:
        return File.AclEntryType.OTHER;
      default:
        throw new IllegalStateException("Unknown AclEntryType: " + entryType);
    }
  }

  /**
   * @param acl the protobuf representation
   * @return {@link AccessControlList}
   */
  public static AccessControlList fromProto(File.AccessControlList acl) {
    AccessControlList ret;
    if (acl.hasIsDefault() && acl.getIsDefault()) {
      ret = new DefaultAccessControlList();
    } else {
      ret = new AccessControlList();
    }
    ret.setOwningUser(acl.getOwningUser());
    ret.setOwningGroup(acl.getOwningGroup());

    if (acl.getIsEmpty()) {
      return ret;
    }

    // true if there are any extended entries (named user or named group)
    boolean hasExtended = false;

    for (File.NamedAclActions namedActions : acl.getUserActionsList()) {
      String name = namedActions.getName();
      AclActions actions = AclActions.fromProtoBuf(namedActions.getActions());
      AclEntry entry;
      if (name.equals(OWNING_USER_KEY)) {
        entry = new AclEntry.Builder().setType(AclEntryType.OWNING_USER)
            .setSubject(acl.getOwningUser()).setActions(actions).build();
      } else {
        hasExtended = true;
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
        hasExtended = true;
        entry = new AclEntry.Builder().setType(AclEntryType.NAMED_GROUP)
            .setSubject(name).setActions(actions).build();
      }
      ret.setEntry(entry);
    }

    if (hasExtended) {
      // Only set the mask if there are any extended acl entries.
      AclActions actions = AclActions.fromProtoBuf(acl.getMaskActions());
      AclEntry entry = new AclEntry.Builder().setType(AclEntryType.MASK)
          .setActions(actions).build();
      ret.setEntry(entry);
    }

    AclActions actions = AclActions.fromProtoBuf(acl.getOtherActions());
    AclEntry entry = new AclEntry.Builder().setType(AclEntryType.OTHER)
        .setActions(actions).build();
    ret.setEntry(entry);

    return ret;
  }

  /**
   * @param action the protobuf representation of {@link AclAction}
   * @return the {@link AclAction} decoded from the protobuf representation
   */
  public static AclAction fromProto(File.AclAction action) {
    switch (action) {
      case READ:
        return AclAction.READ;
      case WRITE:
        return AclAction.WRITE;
      case EXECUTE:
        return AclAction.EXECUTE;
      default:
        throw new IllegalStateException("Unknown protobuf acl action: " + action);
    }
  }

  /**
   * @param pEntry the proto representation
   * @return the {@link AclEntry} instance created from the proto representation
   */
  public static AclEntry fromProto(File.AclEntry pEntry) {
    AclEntry.Builder builder = new AclEntry.Builder();
    builder.setType(fromProto(pEntry.getType()));
    builder.setSubject(pEntry.getSubject());
    builder.setIsDefault(pEntry.getIsDefault());

    for (File.AclAction pAction : pEntry.getActionsList()) {
      builder.addAction(fromProto(pAction));
    }
    return builder.build();
  }

  /**
   * @param pAclEntryType the proto representation
   * @return the {@link AclEntryType} created from the proto representation
   */
  public static AclEntryType fromProto(File.AclEntryType pAclEntryType) {
    switch (pAclEntryType) {
      case OWNER:
        return AclEntryType.OWNING_USER;
      case NAMED_USER:
        return AclEntryType.NAMED_USER;
      case OWNING_GROUP:
        return AclEntryType.OWNING_GROUP;
      case NAMED_GROUP:
        return AclEntryType.NAMED_GROUP;
      case MASK:
        return AclEntryType.MASK;
      case OTHER:
        return AclEntryType.OTHER;
      default:
        throw new IllegalStateException("Unknown proto AclEntryType: " + pAclEntryType);
    }
  }

  /**
   * Converts proto type to wire type.
   *
   * @param pSetAclAction {@link TSetAclAction}
   * @return {@link SetAclAction} equivalent
   */
  public static SetAclAction fromProto(File.SetAclAction pSetAclAction) {
    if (pSetAclAction == null) {
      throw new IllegalStateException("Null proto set acl action.");
    }
    switch (pSetAclAction) {
      case REPLACE:
        return SetAclAction.REPLACE;
      case MODIFY:
        return SetAclAction.MODIFY;
      case REMOVE:
        return SetAclAction.REMOVE;
      case REMOVE_ALL:
        return SetAclAction.REMOVE_ALL;
      case REMOVE_DEFAULT:
        return SetAclAction.REMOVE_DEFAULT;
      default:
        throw new IllegalStateException("Unrecognized proto set acl action: " + pSetAclAction);
    }
  }

  /**
   * Creates a {@link Status} from a protocol buffer type status.
   *
   * @param status the protocol buffer type status
   * @return the corresponding {@link Status}
   */
  public static Status fromProto(alluxio.proto.status.Status.PStatus status) {
    switch (status) {
      case ABORTED:
        return Status.ABORTED;
      case ALREADY_EXISTS:
        return Status.ALREADY_EXISTS;
      case CANCELED:
        return Status.CANCELED;
      case DATA_LOSS:
        return Status.DATA_LOSS;
      case DEADLINE_EXCEEDED:
        return Status.DEADLINE_EXCEEDED;
      case FAILED_PRECONDITION:
        return Status.FAILED_PRECONDITION;
      case INTERNAL:
        return Status.INTERNAL;
      case INVALID_ARGUMENT:
        return Status.INVALID_ARGUMENT;
      case NOT_FOUND:
        return Status.NOT_FOUND;
      case OK:
        return Status.OK;
      case OUT_OF_RANGE:
        return Status.OUT_OF_RANGE;
      case PERMISSION_DENIED:
        return Status.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return Status.RESOURCE_EXHAUSTED;
      case UNAUTHENTICATED:
        return Status.UNAUTHENTICATED;
      case UNAVAILABLE:
        return Status.UNAVAILABLE;
      case UNIMPLEMENTED:
        return Status.UNIMPLEMENTED;
      case UNKNOWN:
        return Status.UNKNOWN;
      default:
        return Status.UNKNOWN;
    }
  }

  /**
   * @return a list of the proto representation of the named users actions
   */
  public List<File.NamedAclActions> getNamedUsersProto(ExtendedACLEntries) {
    List<File.NamedAclActions> actions = new ArrayList<>(mNamedUserActions.size());
    for (Map.Entry<String, AclActions> kv : mNamedUserActions.entrySet()) {
      File.NamedAclActions namedActions = File.NamedAclActions.newBuilder()
          .setName(kv.getKey())
          .setActions(AclActions.toProtoBuf(kv.getValue()))
          .build();
      actions.add(namedActions);
    }
    return actions;
  }

  /**
   * @return a list of the proto representation of the named group actions
   */
  public List<File.NamedAclActions> getNamedGroupsProto(ExtendedACLEntries) {
    List<File.NamedAclActions> actions = new ArrayList<>(mNamedGroupActions.size());
    for (Map.Entry<String, AclActions> kv : mNamedGroupActions.entrySet()) {
      File.NamedAclActions namedActions = File.NamedAclActions.newBuilder()
          .setName(kv.getKey())
          .setActions(AclActions.toProtoBuf(kv.getValue()))
          .build();
      actions.add(namedActions);
    }
    return actions;
  }
}
