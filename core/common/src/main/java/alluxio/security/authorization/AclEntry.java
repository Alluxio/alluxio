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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An entry in {@link AccessControlList}.
 */
public final class AclEntry implements Serializable {
  private static final long serialVersionUID = -738692910777661243L;

  private static final String DEFAULT_KEYWORD = "default";
  private static final String DEFAULT_PREFIX = DEFAULT_KEYWORD + ":";

  /**
   * Type of this entry.
   */
  private AclEntryType mType;

  /**
   * Whether this entry applies to default ACL or not.
   */
  private boolean mIsDefault;

  /**
   * Name of owning user, owning group, named user, named group.
   * If the entry is of type MASK or OTHER, this is an empty string.
   */
  private String mSubject;
  /**
   * Permitted actions.
   */
  private AclActions mActions;

  private AclEntry(AclEntryType type, String subject, AclActions actions, boolean isDefault) {
    mType = type;
    mSubject = subject;
    mActions = actions;
    mIsDefault = isDefault;
  }

  /**
   * @return the type
   */
  public AclEntryType getType() {
    return mType;
  }

  /**
   * @return the subject
   */
  public String getSubject() {
    return mSubject;
  }

  /**
   * @return a copy of actions
   */
  public AclActions getActions() {
    return new AclActions(mActions);
  }

  /**
   * @return whether the action applies to default ACL
   */
  public boolean isDefault() {
    return mIsDefault;
  }

  /**
   * Sets the entry to be default.
   * @param defaultEntry indicating default or not
   */
  public void setDefault(boolean defaultEntry) {
    mIsDefault = defaultEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AclEntry)) {
      return false;
    }
    AclEntry that = (AclEntry) o;
    return Objects.equal(mType, that.mType)
        && Objects.equal(mSubject, that.mSubject)
        && Objects.equal(mActions, that.mActions)
        && Objects.equal(mIsDefault, that.mIsDefault);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mType, mSubject, mActions, mIsDefault);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", mType)
        .add("subject", mSubject)
        .add("actions", mActions)
        .add("isDefault", mIsDefault)
        .toString();
  }

  /**
   * @return the string representation for the CLI
   */
  public String toCliString() {
    StringBuilder sb = new StringBuilder();
    if (mIsDefault) {
      sb.append(DEFAULT_PREFIX);
    }
    sb.append(mType.toCliString());
    sb.append(":");
    if (mType == AclEntryType.NAMED_USER || mType == AclEntryType.NAMED_GROUP) {
      sb.append(mSubject);
    }
    sb.append(":");
    sb.append(mActions.toCliString());

    return sb.toString();
  }

  /**
   * Convert a normal ACL to a string representing a default ACL.
   * @param stringEntry normal ACL, if it is already default ACL, then return the default ACL
   * @return a default ACL
   */
  public static String toDefault(String stringEntry) {
    if (stringEntry == null) {
      throw new IllegalArgumentException("Input acl string is null");
    }
    List<String> components = Arrays.stream(stringEntry.split(":")).map(String::trim).collect(
        Collectors.toList());
    if (components != null && components.size() > 0 && components.get(0).equals(DEFAULT_KEYWORD)) {
      return stringEntry;
    } else {
      return DEFAULT_PREFIX + stringEntry;
    }
  }

  /**
   * Creates an {@link AclEntry} from a string. The possible string inputs are:
   * Owning User: user::[rwx]
   * Named User: user:[user name]:[rwx]
   * Owning Group: group::[rwx]
   * Named Group: group:[group name]:[rwx]
   * Mask: mask::[rwx]
   * Other: other::[rwx]
   * all the above entries, but with default:added to the beginning of the entry
   * default:user::[rwx]
   *
   * [rwx]: all combinations are possible ('---' to 'rwx')
   *
   * @param stringEntry the CLI string representation
   * @return the {@link AclEntry} instance created from the CLI string representation
   */
  public static AclEntry fromCliString(String stringEntry) {
    if (stringEntry == null) {
      throw new IllegalArgumentException("Input acl string is null");
    }
    List<String> components = Arrays.stream(stringEntry.split(":")).map(String::trim).collect(
        Collectors.toList());
    if (!((components.size() == 3 && !components.get(0).equals(DEFAULT_KEYWORD))
        || (components.size() == 4
        && components.get(0).equals(DEFAULT_KEYWORD)))) {
      throw new IllegalArgumentException("Unexpected acl components: " + stringEntry);
    }

    AclEntry.Builder builder = new AclEntry.Builder();

    int startingIndex = 0;
    if (components.get(0).equals(DEFAULT_KEYWORD)) {
      startingIndex = 1;
      builder.setIsDefault(true);
    } else {
      builder.setIsDefault(false);
    }
    String type = components.get(startingIndex + 0);
    String subject = components.get(startingIndex + 1);
    String actions = components.get(startingIndex + 2);

    if (type.isEmpty()) {
      throw new IllegalArgumentException("ACL entry type is empty: " + stringEntry);
    }
    switch (type) {
      case AclEntryType.USER_COMPONENT:
        if (subject.isEmpty()) {
          builder.setType(AclEntryType.OWNING_USER);
        } else {
          builder.setType(AclEntryType.NAMED_USER);
        }
        break;
      case AclEntryType.GROUP_COMPONENT:
        if (subject.isEmpty()) {
          builder.setType(AclEntryType.OWNING_GROUP);
        } else {
          builder.setType(AclEntryType.NAMED_GROUP);
        }
        break;
      case AclEntryType.MASK_COMPONENT:
        if (!subject.isEmpty()) {
          throw new IllegalArgumentException(
              "Subject for acl mask type must be empty: " + stringEntry);
        }
        builder.setType(AclEntryType.MASK);
        break;
      case AclEntryType.OTHER_COMPONENT:
        if (!subject.isEmpty()) {
          throw new IllegalArgumentException(
              "Subject for acl other type must be empty: " + stringEntry);
        }
        builder.setType(AclEntryType.OTHER);
        break;
      default:
        throw new IllegalArgumentException("Unexpected ACL entry type: " + stringEntry);
    }

    builder.setSubject(subject);

    Mode.Bits bits = Mode.Bits.fromString(actions);
    for (AclAction action : bits.toAclActionSet()) {
      builder.addAction(action);
    }
    return builder.build();
  }

  /**
   * Creates an {@link AclEntry} from a string without permissions. The possible string inputs are:
   * Named User: user:USER_NAME[:]
   * Named Group: group:GROUP_NAME[:]
   * Mask: mask[:][:]
   * Default versions of Named User: default:user:USER_NAME[:]
   * Default versions of Named Group: default:group:GROUP_NAME[:]
   * Default version of the mask: default: mask[:][:]
   * @param stringEntry the CLI string representation, without permissions
   * @return the {@link AclEntry} instance created from the CLI string representation
   */
  public static AclEntry fromCliStringWithoutPermissions(String stringEntry) {
    if (stringEntry == null) {
      throw new IllegalArgumentException("Input acl string is null");
    }
    List<String> components = Arrays.stream(stringEntry.split(":")).map(String::trim).collect(
        Collectors.toList());
    if (components.size() < 1 || components.size() > 4
        || (components.size() == 4 && !components.get(0).equals(DEFAULT_KEYWORD))) {
      throw new IllegalArgumentException("Unexpected acl components: " + stringEntry);
    }

    AclEntry.Builder builder = new AclEntry.Builder();
    String type;
    String subject = "";
    String actions = "";
    if (components.get(0).equals(DEFAULT_KEYWORD)) {
      type = components.get(1);
      if (components.size() >= 3) {
        subject = components.get(2);
      }
      if (components.size() >= 4) {
        actions = components.get(3);
      }
      builder.setIsDefault(true);
    } else {
      type = components.get(0);
      if (components.size() >= 2) {
        subject = components.get(1);
      }
      if (components.size() >= 3) {
        actions = components.get(2);
      }
      builder.setIsDefault(false);
    }

    if (!actions.isEmpty()) {
      throw new IllegalArgumentException("ACL permissions cannot be specified: " + stringEntry);
    }
    if (type.isEmpty()) {
      throw new IllegalArgumentException("ACL entry type is empty: " + stringEntry);
    }

    switch (type) {
      case AclEntryType.USER_COMPONENT:
        if (subject.isEmpty()) {
          throw new IllegalArgumentException("ACL entry must have subject: " + stringEntry);
        } else {
          builder.setType(AclEntryType.NAMED_USER);
        }
        break;
      case AclEntryType.GROUP_COMPONENT:
        if (subject.isEmpty()) {
          throw new IllegalArgumentException("ACL entry must have subject: " + stringEntry);
        } else {
          builder.setType(AclEntryType.NAMED_GROUP);
        }
        break;
      case AclEntryType.MASK_COMPONENT:
        if (!subject.isEmpty()) {
          throw new IllegalArgumentException(
              "Subject for acl mask type must be empty: " + stringEntry);
        }
        builder.setType(AclEntryType.MASK);
        break;
      case AclEntryType.OTHER_COMPONENT:
        if (!subject.isEmpty()) {
          throw new IllegalArgumentException(
              "Subject for acl other type must be empty: " + stringEntry);
        }
        builder.setType(AclEntryType.OTHER);
        break;
      default:
        throw new IllegalArgumentException("Unexpected ACL entry type: " + stringEntry);
    }
    builder.setSubject(subject);
    return builder.build();
  }

  /**
   * Builder for {@link AclEntry}.
   */
  public static final class Builder {
    private AclEntryType mType;
    private String mSubject;
    private AclActions mActions;
    private boolean mIsDefault;

    /**
     * Creates a new builder where type is null, subject is an empty string, and no action is
     * permitted.
     */
    public Builder() {
      mSubject = "";
      mActions = new AclActions();
    }

    /**
     * Sets the type of the entry.
     *
     * @param type the type of the entry
     * @return the builder
     */
    public Builder setType(AclEntryType type) {
      mType = type;
      return this;
    }

    /**
     * Sets subject of this entry.
     * If the entry is of type OWNING_USER or NAMED_USER, then the subject is the username.
     * If the entry is of type OWNING_GROUP or NAMED_GROUP, then the subject is the group name.
     * For other types, the subject should be an empty string.
     *
     * @param subject the subject
     * @return the builder
     */
    public Builder setSubject(String subject) {
      mSubject = subject;
      return this;
    }

    /**
     * Sets a copy of actions for this entry.
     *
     * @param actions the actions to be copied from
     * @return the builder
     */
    public Builder setActions(AclActions actions) {
      for (AclAction action : actions.getActions()) {
        mActions.add(action);
      }
      return this;
    }

    /**
     * Adds a permitted action.
     *
     * @param action the permitted action
     * @return the builder
     */
    public Builder addAction(AclAction action) {
      mActions.add(action);
      return this;
    }

    /**
     * Set this AclEntry to be for default ACL.
     * @param isDefault whether this entry is default
     * @return the builder
     */
    public Builder setIsDefault(boolean isDefault) {
      mIsDefault = isDefault;
      return this;
    }

    /**
     * @return a new {@link AclEntry}
     * @throws IllegalStateException if type if null, or if type is either NAMED_USER or NAMED_GROUP
     *    while subject is empty
     */
    public AclEntry build() {
      if (mType == null) {
        throw new IllegalStateException("Type cannot be null");
      }
      boolean subjectRequired = mType.equals(AclEntryType.NAMED_USER)
          || mType.equals(AclEntryType.NAMED_GROUP);
      if (subjectRequired && mSubject.isEmpty()) {
        throw new IllegalStateException("Subject for type " + mType + " cannot be empty");
      }
      return new AclEntry(mType, mSubject, mActions, mIsDefault);
    }
  }
}
