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
public class BlockAllocationOptions {

  /** Size of allocation. */
  private long mSize;
  /** Location requested for allocation. */
  private BlockStoreLocation mLocation;
  /** Whether the given location is to be enforced. */
  private boolean mForceLocation;
  /** Whether eviction is allowed to satisfy this allocation. */
  private boolean mEvictionAllowed;

  private BlockAllocationOptions(BlockStoreLocation location, long size) {
    mLocation = location;
    mSize = size;
  }

  /**
   * Creates default allocation options for block create request.
   *
   * @param sizeBytes size of allocation
   * @param location location of allocation
   * @return the allocation object initialized with defaults for create
   */
  public static BlockAllocationOptions defaultsForCreate(long sizeBytes,
      BlockStoreLocation location) {
    return new BlockAllocationOptions(location, sizeBytes)
        .setForceLocation(false)
        .setEvictionAllowed(true);
  }

  /**
   * Creates default allocation options for requesting more space for a block.
   *
   * @param sizeBytes size of allocation
   * @param location location of allocation
   * @return the allocation object initialized with defaults for requesting space
   */
  public static BlockAllocationOptions defaultsForRequest(long sizeBytes,
      BlockStoreLocation location) {
    return new BlockAllocationOptions(location, sizeBytes)
        .setForceLocation(true)
        .setEvictionAllowed(true);
  }

  /**
   * Creates default allocation options for moving a block.
   *
   * @param sizeBytes size of allocation
   * @param location location of allocation
   * @return the allocation object initialized with defaults for moving
   */
  public static BlockAllocationOptions defaultsForMove(long sizeBytes,
      BlockStoreLocation location) {
    return new BlockAllocationOptions(location, sizeBytes)
        .setForceLocation(true)
        .setEvictionAllowed(false);
  }

  /**
   * Sets value for whether to enforce location of allocation.
   *
   * @param forceLocation force location
   * @return the updated options
   */
  public BlockAllocationOptions setForceLocation(boolean forceLocation) {
    mForceLocation = forceLocation;
    return this;
  }

  /**
   * Sets value for whether eviction is allowed for allocation.
   *
   * @param evictionAllowed eviction allowed
   * @return the updated options
   */
  public BlockAllocationOptions setEvictionAllowed(boolean evictionAllowed) {
    mEvictionAllowed = evictionAllowed;
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

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof BlockAllocationOptions)) {
      return false;
    }
    BlockAllocationOptions other = (BlockAllocationOptions) o;
    return mSize == other.mSize
            && mLocation.equals(other.mLocation)
            && mForceLocation == other.mForceLocation
            && mEvictionAllowed == other.mEvictionAllowed;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mSize, mLocation, mForceLocation, mEvictionAllowed);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("Location", mLocation)
        .add("Size", mSize)
        .add("ForceLocation", mForceLocation)
        .add("EvictionAllowed", mEvictionAllowed)
        .toString();
  }
}
