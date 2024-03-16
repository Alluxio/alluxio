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

package alluxio.uri;

/**
 * A multi-master authority implementation.
 */
public class MultiMasterAuthority implements Authority {
  private static final long serialVersionUID = 2580736424809131651L;
  private static final char SEPARATOR = ',';

  /**
   * The master addresses transform from the original authority string.
   * Semicolons and plus signs in authority are replaced by commas to match our
   * internal property format.
   */
  private final String mMasterAddresses;
  private int mHashCode = 0;

  /**
   * @param masterAddresses the multi master addresses
   */
  public MultiMasterAuthority(String masterAddresses) {
    mMasterAddresses = masterAddresses;
  }

  /**
   * @return the Alluxio master addresses from the authority
   */
  public String getMasterAddresses() {
    return mMasterAddresses;
  }

  @Override
  public int compareTo(Authority other) {
    if (equals(other)) {
      return 0;
    }
    return toString().compareTo(other.toString());
  }

  /**
   * Checks if two {@code MultiMasterAuthority}s contain the same set of masters.
   * The order of the masters are not important, i.e. {@code "host1:port1,host2:port2"} and
   * {@code "host2:port2,host1:port2"} are equal.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MultiMasterAuthority)) {
      return false;
    }
    MultiMasterAuthority that = (MultiMasterAuthority) o;
    // first check if the strings are equal
    if (mMasterAddresses.equals(that.mMasterAddresses)) {
      return true;
    }
    // then check if the strings are equal in length
    if (mMasterAddresses.length() != that.mMasterAddresses.length()) {
      return false;
    }
    // then check if the numbers of segments are equal
    int numSegThis = 0;
    int numSegThat = 0;
    for (int i = 0; i < mMasterAddresses.length(); i++) {
      if (mMasterAddresses.charAt(i) == SEPARATOR) {
        numSegThis++;
      }
      if (that.mMasterAddresses.charAt(i) == SEPARATOR) {
        numSegThat++;
      }
    }
    if (numSegThat != numSegThis) {
      return false;
    }
    // then check the masters segments ignoring order
    return equalsUnordered(that.mMasterAddresses);
  }

  private boolean equalsUnordered(String them) {
    String me = mMasterAddresses;
    // find all segments of them in me
    if (!containsAllSegments(me, them)) {
      return false;
    }
    // find all segments of me in them
    return containsAllSegments(them, me);
  }

  /**
   * Checks if all segments separated by a comma ({@code ,}) in {@code needles} are
   * contained in {@code haystack}.
   * <p>
   * This method uses {@code O(1)} space and {@code O(nm)} time, where {@code n} and {@code m}
   * are lengths of the haystack and needles strings, respectively.
   * This method does not allocate temporary objects.
   * @param haystack the template within which to look for the segments in {@code needles}
   * @param needles the segments separated by a comma
   * @return true if all segments are found in {@code haystack}, false otherwise
   */
  private static boolean containsAllSegments(String haystack, String needles) {
    // iterate through all segments in needles
    int needlesSegmentStart;
    int needlesSegmentEnd;
    for (needlesSegmentStart = 0;
         needlesSegmentStart < needles.length();
         needlesSegmentStart = needlesSegmentEnd + 1) {
      needlesSegmentEnd = nextSegmentEnd(needles, needlesSegmentStart);
      // iterate through all segments in haystack
      int haystackSegmentStart;
      int haystackSegmentEnd;
      boolean found = false;
      for (haystackSegmentStart = 0;
           haystackSegmentStart < haystack.length();
           haystackSegmentStart = haystackSegmentEnd + 1) {
        haystackSegmentEnd = nextSegmentEnd(haystack, haystackSegmentStart);
        int haystackSegmentLen = haystackSegmentEnd - haystackSegmentStart;
        int needlesSegmentLen = needlesSegmentEnd - needlesSegmentStart;
        // check if the segments are equal in length, skip to next segment if not equal
        if (haystackSegmentLen != needlesSegmentLen) {
          continue;
        }
        // lengths match, then compare the contents
        if (haystack.regionMatches(
            haystackSegmentStart, needles, needlesSegmentStart, haystackSegmentLen)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets the end index of next segment in {@code segments}, starting from index {@code start}.
   * If already at the final segment, returns the end index of the string.
   */
  private static int nextSegmentEnd(String segments, int start) {
    int end = segments.indexOf(SEPARATOR, start);
    if (end == -1) {
      return segments.length();
    }
    return end;
  }

  @Override
  public int hashCode() {
    // rationale:
    // 1. calculate each comma-separated segment's hashcode in the same way
    //    Java calculates the hashcode of a String object.
    // 2. then XOR each segment's hashcode to produce the hashcode for this object.
    //    since XOR is commutative, the order of the segments does not matter.
    //    as long as the number of the segments and the hashcodes of the segments are equal,
    //    the resulting hashcodes are equal.

    // we might need to update the hashcode field,
    // so copy to a local variable to avoid data race
    int h = mHashCode;
    if (mMasterAddresses.isEmpty() || h != 0) {
      // either the string is empty, so the hashcode is 0,
      // or we have calculated and cached the hashcode
      return h;
    }
    final int prime = 31;
    int segmentHashCode;
    int segmentStart;
    int segmentEnd;
    for (segmentStart = 0;
         segmentStart < mMasterAddresses.length();
         segmentStart = segmentEnd + 1) {
      segmentEnd = nextSegmentEnd(mMasterAddresses, segmentStart);
      segmentHashCode = 0;
      for (int i = segmentStart; i < segmentEnd; i++) {
        segmentHashCode = prime * segmentHashCode + mMasterAddresses.charAt(i);
      }
      h ^= segmentHashCode;
    }

    mHashCode = h;
    return h;
  }

  @Override
  public String toString() {
    return mMasterAddresses;
  }
}
