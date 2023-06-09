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

package alluxio.master.file.mdsync;

import alluxio.AlluxioURI;

import java.util.Objects;

/**
 * A path sequence.
 */
public class PathSequence {
  private final AlluxioURI mStart;
  private final AlluxioURI mEnd;

  /**
   * Creates a path sequence.
   * @param start the start path
   * @param end the end path
   */
  public PathSequence(AlluxioURI start, AlluxioURI end) {
    mStart = start;
    mEnd = end;
  }

  AlluxioURI getStart() {
    return mStart;
  }

  AlluxioURI getEnd() {
    return mEnd;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PathSequence that = (PathSequence) o;
    return Objects.equals(mStart, that.mStart) && Objects.equals(mEnd, that.mEnd);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mStart, mEnd);
  }
}
