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

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.grpc.Bits;
import alluxio.grpc.PMode;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * POSIX style file/directory access mode.
 */
@PublicApi
@NotThreadSafe
public final class Mode {
  private Bits mOwnerBits;
  private Bits mGroupBits;
  private Bits mOtherBits;

  /**
   * Gets the default mode.
   *
   * @return the default {@link Mode}
   */
  public static Mode defaults() {
    return new Mode(Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  /**
   * Creates the "no access" mode.
   *
   * @return the none {@link Mode}
   */
  public static Mode createNoAccess() {
    return new Mode();
  }

  /**
   * Creates the "full access" mode.
   *
   * @return the none {@link Mode}
   */
  public static Mode createFullAccess() {
    return new Mode(Bits.ALL, Bits.ALL, Bits.ALL);
  }

  /**
   * Default constructor for {@link Mode}. Default constructor is required for equality testing
   * and JSON deserialization.
   */
  private Mode() {
    mOwnerBits = Bits.NONE;
    mGroupBits = Bits.NONE;
    mOtherBits = Bits.NONE;
  }

  /**
   * Constructs an instance of {@link Mode} with the given {@link Bits}.
   *
   * @param ownerBits the owner {@link Bits}
   * @param groupBits the group {@link Bits}
   * @param otherBits the other {@link Bits}
   */
  public Mode(Bits ownerBits, Bits groupBits, Bits otherBits) {
    set(ownerBits, groupBits, otherBits);
  }

  /**
   * Constructs an instance of {@link Mode} with the given mode.
   *
   * @param mode the digital representation of a {@link Mode}
   * @see #toShort()
   */
  public Mode(short mode) {
    fromShort(mode);
  }

  /**
   * Copy constructor.
   *
   * @param mode another {@link Mode}
   */
  public Mode(Mode mode) {
    set(mode.mOwnerBits, mode.mGroupBits, mode.mOtherBits);
  }

  /**
   * @param mode1 first mode of the and operation
   * @param mode2 second mode of the and operation
   * @return the AND result of the two Modes
   */
  public static Mode and(Mode mode1, Mode mode2) {
    Bits u = mode1.mOwnerBits.and(mode2.mOwnerBits);
    Bits g = mode1.mGroupBits.and(mode2.mGroupBits);
    Bits o = mode1.mOtherBits.and(mode2.mOtherBits);
    return new Mode(u, g, o);
  }

  /**
   * @return the owner {@link Bits}
   */
  public Bits getOwnerBits() {
    return mOwnerBits;
  }

  /**
   * @param mode the digital representation of a {@link Mode}
   * @return the owner {@link Bits}
   */
  public static Bits extractOwnerBits(short mode) {
    return Bits.values()[(mode >>> 6) & 7];
  }

  /**
   * Sets owner bits.
   *
   * @param bits the owner bits to set
   */
  public void setOwnerBits(Bits bits) {
    mOwnerBits = bits;
  }

  /**
   * @return the group {@link Bits}
   */
  public Bits getGroupBits() {
    return mGroupBits;
  }

  /**
   * @param mode the digital representation of a {@link Mode}
   * @return the group {@link Bits}
   */
  public static Bits extractGroupBits(short mode) {
    return Bits.values()[(mode >>> 3) & 7];
  }

  /**
   * Sets group bits.
   *
   * @param bits the group bits to set
   */
  public void setGroupBits(Bits bits) {
    mGroupBits = bits;
  }

  /**
   * @return the other {@link Bits}
   */
  public Bits getOtherBits() {
    return mOtherBits;
  }

  /**
   * @param mode the digital representation of a {@link Mode}
   * @return the other {@link Bits}
   */
  public static Bits extractOtherBits(short mode) {
    return Bits.values()[mode & 7];
  }

  /**
   * Sets other bits.
   *
   * @param bits the other bits to set
   */
  public void setOtherBits(Bits bits) {
    mOtherBits = bits;
  }

  private void set(Bits u, Bits g, Bits o) {
    mOwnerBits = u;
    mGroupBits = g;
    mOtherBits = o;
  }

  /**
   * Sets {@link Mode} bits using a digital representation.
   *
   * @param n the digital representation of a {@link Mode}
   */
  public void fromShort(short n) {
    Bits[] v = Bits.values();
    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7]);
  }

  /**
   * Encodes the object as a short.
   *
   * @return the digital representation of this {@link Mode}
   */
  public short toShort() {
    int s = (mOwnerBits.ordinal() << 6) | (mGroupBits.ordinal() << 3) | mOtherBits.ordinal();
    return (short) s;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Mode) {
      Mode that = (Mode) obj;
      return mOwnerBits == that.mOwnerBits && mGroupBits == that.mGroupBits
          && mOtherBits == that.mOtherBits;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toShort();
  }

  @Override
  public String toString() {
    return mOwnerBits.toString() + mGroupBits.toString() + mOtherBits.toString();
  }

  /**
   * Creates {@link Mode} from proto {@link PMode}.
   *
   * @param pMode proto mode
   * @return created mode
   */
  public static Mode fromProto(PMode pMode) {
    Bits ownerBits = pMode.hasOwnerBits() ? Bits.valueOf(pMode.getOwnerBits().name()) : Bits.NONE;
    Bits groupBits = pMode.hasGroupBits() ? Bits.valueOf(pMode.getGroupBits().name()) : Bits.NONE;
    Bits otherBits = pMode.hasOtherBits() ? Bits.valueOf(pMode.getOtherBits().name()) : Bits.NONE;
    return new Mode(ownerBits, groupBits, otherBits);
  }

  /**
   * @return proto representation of this mode instance
   */
  public PMode toProto() {
    return PMode.newBuilder()
        .setOwnerBits(alluxio.grpc.Bits.valueOf(mOwnerBits.name()))
        .setGroupBits(alluxio.grpc.Bits.valueOf(mGroupBits.name()))
        .setOtherBits(alluxio.grpc.Bits.valueOf(mOtherBits.name()))
        .build();
  }

  /**
   * Mode bits.
   */
  @PublicApi
  @ThreadSafe
  public enum Bits {
    NONE(Constants.MODE_BITS_NONE),
    EXECUTE(Constants.MODE_BITS_EXECUTE),
    WRITE(Constants.MODE_BITS_WRITE),
    WRITE_EXECUTE(Constants.MODE_BITS_WRITE_EXECUTE),
    READ(Constants.MODE_BITS_READ),
    READ_EXECUTE(Constants.MODE_BITS_READ_EXECUTE),
    READ_WRITE(Constants.MODE_BITS_READ_WRITE),
    ALL(Constants.MODE_BITS_ALL),
    ;

    /** String representation of the bits. */
    private final String mString;

    /** Retain reference to the values array. */
    private static final Bits[] SVALS = values();

    Bits(String s) {
      mString = s;
    }

    /**
     * Creates a {@link Bits} from a short.
     *
     * @param bits {@link Bits} in short
     * @return the {@link Bits} created
     */
    public static Bits fromShort(short bits) {
      return SVALS[bits];
    }

    /**
     * @param string the string representation
     * @return the {@link Bits} instance
     */
    public static Bits fromString(String string) {
      switch (string) {
        case Constants.MODE_BITS_NONE:
          return NONE;
        case Constants.MODE_BITS_EXECUTE:
          return EXECUTE;
        case Constants.MODE_BITS_WRITE:
          return WRITE;
        case Constants.MODE_BITS_WRITE_EXECUTE:
          return WRITE_EXECUTE;
        case Constants.MODE_BITS_READ:
          return READ;
        case Constants.MODE_BITS_READ_EXECUTE:
          return READ_EXECUTE;
        case Constants.MODE_BITS_READ_WRITE:
          return READ_WRITE;
        case Constants.MODE_BITS_ALL:
          return ALL;
        default:
          throw new IllegalArgumentException("Invalid mode string: " + string);
      }
    }

    /**
     * @param protoBits the proto bits
     * @return created instance from proto representation
     */
    public static Bits fromProto(alluxio.grpc.Bits protoBits) {
      return Bits.valueOf(protoBits.name());
    }

    /**
     * @return the proto representation of Bits
     */
    public alluxio.grpc.Bits toProto() {
      return alluxio.grpc.Bits.valueOf(name());
    }

    @Override
    public String toString() {
      return mString;
    }

    /**
     * Checks whether these bits imply the given bits.
     *
     * @param that mode bits
     * @return true when these bits imply the given bits
     */
    public boolean imply(Bits that) {
      if (that != null) {
        return (ordinal() & that.ordinal()) == that.ordinal();
      }
      return false;
    }

    /**
     * @param that mode bits
     * @return the intersection of thes bits and the given bits
     */
    public Bits and(Bits that) {
      Preconditions.checkNotNull(that, "that");
      return SVALS[ordinal() & that.ordinal()];
    }

    /**
     * @param that mode bits
     * @return the union of thes bits and the given bits
     */
    public Bits or(Bits that) {
      Preconditions.checkNotNull(that, "that");
      return SVALS[ordinal() | that.ordinal()];
    }

    /**
     * @return the complement of these bits
     */
    public Bits not() {
      return SVALS[7 - ordinal()];
    }

    /**
     * @return the set of {@link AclAction}s implied by this mode
     */
    public Set<AclAction> toAclActionSet() {
      Set<AclAction> actions = new HashSet<>();
      if (imply(READ)) {
        actions.add(AclAction.READ);
      }
      if (imply(WRITE)) {
        actions.add(AclAction.WRITE);
      }
      if (imply(EXECUTE)) {
        actions.add(AclAction.EXECUTE);
      }
      return actions;
    }

    /**
     * @return the set of {@link AclAction}s implied by this mode
     */
    public AclActions toAclActions() {
      AclActions actions = new AclActions();
      if (imply(READ)) {
        actions.add(AclAction.READ);
      }
      if (imply(WRITE)) {
        actions.add(AclAction.WRITE);
      }
      if (imply(EXECUTE)) {
        actions.add(AclAction.EXECUTE);
      }
      return actions;
    }
  }
}
