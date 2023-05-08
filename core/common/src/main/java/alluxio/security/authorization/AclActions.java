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
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Permitted actions of an entry in {@link AccessControlList}.
 */
@NotThreadSafe
public final class AclActions implements Serializable {
  private static final long serialVersionUID = 4500558548535992938L;

  /** An index of the bitset equals an ordinal of an action in {@link AclAction}. */
  private Mode.Bits mActions = Mode.Bits.NONE;

  /**
   * Creates a new instance where no action is permitted.
   */
  public AclActions() {}

  /**
   * Creates a new instance from the mode.
   * @param mode the mode
   */
  public AclActions(Mode.Bits mode) {
    mActions = mode;
  }

  /**
   * Creates a copy of actions.
   *
   * @param actions the actions to be copied from
   */
  public AclActions(AclActions actions) {
    mActions = actions.mActions;
  }

  /**
   * Creats a new instance with initial permitted actions.
   *
   * @param actions the initial setEntry of permitted actions which is copied from
   */
  public AclActions(Set<AclAction> actions) {
    for (AclAction action : actions) {
      mActions = mActions.or(action.toBit());
    }
  }

  /**
   * @return an immutable copy of permitted actions
   */
  public Set<AclAction> getActions() {
    return mActions.toAclActionSet();
  }

  /**
   * Updates permitted actions based on the mode bits.
   *
   * For example, if bits imply READ, then READ is added to permitted actions, otherwise,
   * READ is removed from permitted actions.
   * Same logic applies to WRITE and EXECUTE.
   *
   * @param bits the mode bits
   */
  public void updateByModeBits(Mode.Bits bits) {
    // Each index equals to corresponding AclAction's ordinal.
    // E.g. Mode.Bits.READ corresponds to AclAction.READ, the former has index 0 in indexedBits,
    // the later has ordinal 0 in AclAction.
    mActions = bits;
  }

  /**
   * Adds a permitted action.
   *
   * @param action the permitted action
   */
  public void add(AclAction action) {
    mActions = mActions.or(action.toBit());
  }

  /**
   * Merges the actions. (Or operation)
   *
   * @param actions the actions to be merged from
   */
  public void merge(AclActions actions) {
    mActions = mActions.or(actions.mActions);
  }

  /**
   * Mask the actions. (And operation)
   * @param actions the acl mask
   */
  public void mask(AclActions actions)  {
    mActions = mActions.and(actions.mActions);
  }

  /**
   * @param action the action to be checked
   * @return whether the action is contained in the permitted actions
   */
  public boolean contains(AclAction action) {
    return mActions.imply(action.toBit());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AclActions)) {
      return false;
    }
    AclActions that = (AclActions) o;
    return Objects.equal(mActions, that.mActions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mActions);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("actions", getActions())
        .toString();
  }

  /**
   * @return the string representation for the CLI
   */
  public String toCliString() {
    return toModeBits().toString();
  }

  /**
   * @return the representation of the permitted actions in the format of {@link Mode.Bits}
   */
  public Mode.Bits toModeBits() {
    return mActions;
  }
}
