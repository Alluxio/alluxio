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

package alluxio.worker.block;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Used to specify various options while allocating space.
 */
public class AllocateOptions {
  /** Size of allocation. */
  private long mSize;
  /** Location requested for allocation. */
  private BlockStoreLocation mLocation;
  /** Whether the given location is to be enforced. */
  private boolean mForceLocation;
  /** Whether eviction is allowed to satisfy this allocation. */
  private boolean mEvictionAllowed;
  /** Whether to use the reserved space for allocation. */
  private boolean mUseReservedSpace;

  /**
   * Creates new allocation options object.
   *
   * @param location the allocation location
   * @param size the allocation size
   */
  private AllocateOptions(BlockStoreLocation location, long size) {
    this(location);
    mSize = size;
  }

  /**
   * Creates new allocation options object.
   *
   * @param location the allocation location
   */
  private AllocateOptions(BlockStoreLocation location) {
    mLocation = location;
    mUseReservedSpace = false;
  }

  /**
   * Creates default allocation options for block create request.
   *  - Locations is not strict
   *  - Evicting on destination is allowed
   *
   * @param sizeBytes size of allocation
   * @param location location of allocation
   * @return the allocation object initialized with defaults for create
   */
  public static AllocateOptions forCreate(long sizeBytes, BlockStoreLocation location) {
    return new AllocateOptions(location, sizeBytes)
        .setForceLocation(false)
        .setEvictionAllowed(true);
  }

  /**
   * Creates default allocation options for requesting more space for a block.
   *  - Locations is strict
   *  - Evicting on destination is allowed
   *
   * @param sizeBytes size of allocation
   * @param location location of allocation
   * @return the allocation object initialized with defaults for requesting space
   */
  public static AllocateOptions forRequestSpace(long sizeBytes, BlockStoreLocation location) {
    return new AllocateOptions(location, sizeBytes)
        .setForceLocation(true)
        .setEvictionAllowed(true);
  }

  /**
   * Creates default allocation options for moving a block during tier-move task.
   *  - Locations is strict
   *  - Evicting on destination is disallowed
   *
   * @param location location of allocation
   * @return the allocation object initialized with defaults for moving
   */
  public static AllocateOptions forTierMove(BlockStoreLocation location) {
    return new AllocateOptions(location)
        .setForceLocation(true)
        .setEvictionAllowed(false);
  }

  /**
   * Creates default allocation options for moving a block by a client request.
   *  - Locations is strict
   *  - Evicting on destination is allowed
   *
   * @param location location of allocation
   * @return the allocation object initialized with defaults for moving
   */
  public static AllocateOptions forMove(BlockStoreLocation location) {
    return new AllocateOptions(location)
        .setForceLocation(true)
        .setEvictionAllowed(true);
  }

  /**
   * Sets the allocation location.
   *
   * @param location the allocation location
   * @return the updated options
   */
  public AllocateOptions setLocation(BlockStoreLocation location) {
    mLocation = location;
    return this;
  }

  /**
   * Sets the allocation size.
   *
   * @param size the allocation size in bytes
   * @return the updated options
   */
  public AllocateOptions setSize(long size) {
    mSize = size;
    return this;
  }

  /**
   * Sets value for whether to enforce location of allocation.
   *
   * @param forceLocation force location
   * @return the updated options
   */
  public AllocateOptions setForceLocation(boolean forceLocation) {
    mForceLocation = forceLocation;
    return this;
  }

  /**
   * Sets value for whether eviction is allowed for allocation.
   *
   * @param evictionAllowed eviction allowed
   * @return the updated options
   */
  public AllocateOptions setEvictionAllowed(boolean evictionAllowed) {
    mEvictionAllowed = evictionAllowed;
    return this;
  }

  /**
   * Sets value for whether this allocation can use reserved space.
   *
   * @param useReservedSpace use reserved space
   * @return the updated options
   */
  public AllocateOptions setUseReservedSpace(boolean useReservedSpace) {
    mUseReservedSpace = useReservedSpace;
    return this;
  }

  /**
   * @return the location of allocation
   */
  public BlockStoreLocation getLocation() {
    return mLocation;
  }

  /**
   * @return the size of allocation
   */
  public long getSize() {
    return mSize;
  }

  /**
   * @return whether location is to be enforced
   */
  public boolean isForceLocation() {
    return mForceLocation;
  }

  /**
   * @return whether eviction is allowed for allocation
   */
  public boolean isEvictionAllowed() {
    return mEvictionAllowed;
  }

  /**
   * @return whether this allocation can use reserved space
   */
  public boolean canUseReservedSpace() {
    return mUseReservedSpace;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof AllocateOptions)) {
      return false;
    }
    AllocateOptions other = (AllocateOptions) o;
    return mSize == other.mSize
        && mLocation.equals(other.mLocation)
        && mForceLocation == other.mForceLocation
        && mEvictionAllowed == other.mEvictionAllowed
        && mUseReservedSpace == other.mUseReservedSpace;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSize, mLocation, mForceLocation, mEvictionAllowed, mUseReservedSpace);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Location", mLocation)
        .add("Size", mSize)
        .add("ForceLocation", mForceLocation)
        .add("EvictionAllowed", mEvictionAllowed)
        .add("UseReservedSpace", mUseReservedSpace)
        .toString();
  }
}
