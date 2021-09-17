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
import com.google.common.collect.ImmutableSet;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Permitted actions of an entry in {@link AccessControlList}.
 */
@NotThreadSafe
public final class AclActions implements Serializable {
  private static final long serialVersionUID = 4500558548535992938L;

  // TODO(ohboring): have a static default AclActions object, and then copy on write.
  /**
   * Initial bits of the actions bitset.
   * Since most of the time, there are at most 3 actions: read, write, and execute,
   * so it's set to 3.
   */
  private static final int ACTIONS_BITSET_INITIAL_BITS = 3;

  /** An index of the bitset equals an ordinal of an action in {@link AclAction}. */
  private BitSet mActions;

  /**
   * Creates a new instance where no action is permitted.
   */
  public AclActions() {
    mActions = new BitSet(ACTIONS_BITSET_INITIAL_BITS);
  }

  /**
   * Creates a copy of actions.
   *
   * @param actions the actions to be copied from
   */
  public AclActions(AclActions actions) {
    mActions = (BitSet) actions.mActions.clone();
  }

  /**
   * Creats a new instance with initial permitted actions.
   *
   * @param actions the initial setEntry of permitted actions which is copied from
   */
  public AclActions(Set<AclAction> actions) {
    mActions = new BitSet(ACTIONS_BITSET_INITIAL_BITS);
    for (AclAction action : actions) {
      mActions.set(action.ordinal());
    }
  }

  /**
   * @return an immutable copy of permitted actions
   */
  public Set<AclAction> getActions() {
    ImmutableSet.Builder<AclAction> builder = ImmutableSet.builder();
    for (int i = mActions.nextSetBit(0); i >= 0; i = mActions.nextSetBit(i + 1)) {
      builder.add(AclAction.ofOrdinal(i));
    }
    return builder.build();
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
    Mode.Bits[] indexedBits = new Mode.Bits[]{
        Mode.Bits.READ, Mode.Bits.WRITE, Mode.Bits.EXECUTE
    };
    for (int i = 0; i < 3; i++) {
      if (bits.imply(indexedBits[i])) {
        mActions.set(i);
      } else {
        mActions.clear(i);
      }
    }
  }

  /**
   * Adds a permitted action.
   *
   * @param action the permitted action
   */
  public void add(AclAction action) {
    mActions.set(action.ordinal());
  }

  /**
   * Merges the actions. (Or operation)
   *
   * @param actions the actions to be merged from
   */
  public void merge(AclActions actions) {
    mActions.or(actions.mActions);
  }

  /**
   * Mask the actions. (And operation)
   * @param actions the acl mask
   */
  public void mask(AclActions actions)  {
    mActions.and(actions.mActions);
  }

  /**
   * @param action the action to be checked
   * @return whether the action is contained in the permitted actions
   */
  public boolean contains(AclAction action) {
    return mActions.get(action.ordinal());
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
    Mode.Bits bits = Mode.Bits.NONE;
    if (contains(AclAction.READ)) {
      bits = bits.or(Mode.Bits.READ);
    }
    if (contains(AclAction.WRITE)) {
      bits = bits.or(Mode.Bits.WRITE);
    }
    if (contains(AclAction.EXECUTE)) {
      bits = bits.or(Mode.Bits.EXECUTE);
    }
    return bits;
  }
}
