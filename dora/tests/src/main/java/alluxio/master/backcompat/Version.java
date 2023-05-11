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

package alluxio.master.backcompat;

import com.google.common.base.Objects;

/**
 * Class for representing a version.
 */
public final class Version implements Comparable<Version> {
  private final int mMajor;
  private final int mMinor;
  private final int mPatch;

  /**
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   */
  public Version(int major, int minor, int patch) {
    mMajor = major;
    mMinor = minor;
    mPatch = patch;
  }

  @Override
  public int compareTo(Version o) {
    if (mMajor != o.mMajor) {
      return mMajor - o.mMajor;
    }
    if (mMinor != o.mMinor) {
      return mMinor - o.mMinor;
    }
    if (mPatch != o.mPatch) {
      return mPatch - o.mPatch;
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (!(o instanceof Version)) {
      return false;
    }

    Version other = (Version) o;

    return compareTo(other) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMajor, mMinor, mPatch);
  }

  @Override
  public String toString() {
    return String.format("%d.%d.%d", mMajor, mMinor, mPatch);
  }
}
