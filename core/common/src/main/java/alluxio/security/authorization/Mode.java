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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.exception.ExceptionMessage;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * POSIX style file/directory access mode.
 */
@PublicApi
@NotThreadSafe
public final class Mode {
  private static final Mode FILE_UMASK = new Mode(Constants.FILE_DIR_PERMISSION_DIFF);

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
   * Applies the default umask for newly created files to this mode.
   *
   * @return the updated object
   */
  public Mode applyFileUMask() {
    return applyUMask(Mode.getUMask()).applyUMask(FILE_UMASK);
  }

  /**
   * Applies the default umask for newly created directories to this mode.
   *
   * @return the updated object
   */
  public Mode applyDirectoryUMask() {
    return applyUMask(Mode.getUMask());
  }

  /**
   * Applies the given umask {@link Mode} to this mode.
   *
   * @param umask the umask to apply
   * @return the updated object
   */
  private Mode applyUMask(Mode umask) {
    mOwnerBits = mOwnerBits.and(umask.mOwnerBits.not());
    mGroupBits = mGroupBits.and(umask.mGroupBits.not());
    mOtherBits = mOtherBits.and(umask.mOtherBits.not());
    return this;
  }

  /**
   * Gets the file / directory creation umask.
   *
   * @return the umask {@link Mode}
   */
  private static Mode getUMask() {
    int umask = Constants.DEFAULT_FILE_SYSTEM_UMASK;
    String confUmask = Configuration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK);
    if (confUmask != null) {
      if ((confUmask.length() > 4) || !isValid(confUmask)) {
        throw new IllegalArgumentException(ExceptionMessage.INVALID_CONFIGURATION_VALUE
            .getMessage(confUmask, PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
      }
      int newUmask = 0;
      int lastIndex = confUmask.length() - 1;
      for (int i = 0; i <= lastIndex; i++) {
        newUmask += (confUmask.charAt(i) - '0') << 3 * (lastIndex - i);
      }
      umask = newUmask;
    }
    return new Mode((short) umask);
  }

  private static boolean isValid(String value) {
    try {
      Integer.parseInt(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Mode bits.
   */
  @PublicApi
  @ThreadSafe
  public enum Bits {
    NONE("---"),
    EXECUTE("--x"),
    WRITE("-w-"),
    WRITE_EXECUTE("-wx"),
    READ("r--"),
    READ_EXECUTE("r-x"),
    READ_WRITE("rw-"),
    ALL("rwx"),
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
      Preconditions.checkNotNull(that);
      return SVALS[ordinal() & that.ordinal()];
    }

    /**
     * @param that mode bits
     * @return the union of thes bits and the given bits
     */
    public Bits or(Bits that) {
      Preconditions.checkNotNull(that);
      return SVALS[ordinal() | that.ordinal()];
    }

    /**
     * @return the complement of these bits
     */
    public Bits not() {
      return SVALS[7 - ordinal()];
    }
  }
}
