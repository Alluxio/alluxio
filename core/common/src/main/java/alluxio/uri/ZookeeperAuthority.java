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
 * {@link ZookeeperAuthority} supports authority containing Zookeeper addresses.
 */
public final class ZookeeperAuthority implements Authority {
  private static final long serialVersionUID = -3549197285125519688L;
  private static final char SEPARATOR = ',';

  private final String mZkAddress;
  private int mHashCode = 0;

  /**
   * @param zkAddress the zookeeper address inside the uri
   */
  public ZookeeperAuthority(String zkAddress) {
    mZkAddress = zkAddress;
  }

  /**
   * @return the Zookeeper address in this authority
   */
  public String getZookeeperAddress() {
    return mZkAddress;
  }

  @Override
  public int compareTo(Authority other) {
    if (equals(other))
    {
      return 0;
    }
    return toString().compareTo(other.toString());
  }

  /**
   * Checks if two {@code ZookeeperAuthority}s contain the same set of addresses.
   * The order of the zookeepers are not important, i.e. {@code "host1:port1,host2:port2"} and
   * {@code "host2:port2,host1:port2"} are equal.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ZookeeperAuthority)) {
      return false;
    }
    ZookeeperAuthority that = (ZookeeperAuthority) o;

    if (mZkAddress.equals(that.mZkAddress))
    {
      return true;
    }

    if (mZkAddress.length() != that.mZkAddress.length())
    {
      return false;
    }

    int numSegThis = 0;
    int numSegThat = 0;

    for (int i = 0; i < mZkAddress.length(); i++)
    {
      if (mZkAddress.charAt(i) == SEPARATOR)
      {
        numSegThis++;
      }
      if (that.mZkAddress.charAt(i) == SEPARATOR)
      {
        numSegThat++;
      }
    }

    if (numSegThis != numSegThat)
    {
      return false;
    }

    return equalsUnordered(that.mZkAddress);
  }

  private boolean equalsUnordered(String them) {
    String me = mZkAddress;
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
    if (h != 0) {
      // if we have calculated and cached the hashcode, just return it
      return h;
    }
    final int prime = 31;
    int segmentHashCode;
    int segmentStart;
    int segmentEnd;
    for (segmentStart = 0;
         segmentStart < mZkAddress.length();
         segmentStart = segmentEnd + 1) {
      segmentEnd = nextSegmentEnd(mZkAddress, segmentStart);
      segmentHashCode = 0;
      for (int i = segmentStart; i < segmentEnd; i++) {
        segmentHashCode = prime * segmentHashCode + mZkAddress.charAt(i);
      }
      h ^= segmentHashCode;
    }

    mHashCode = h;
    return h;
  }

  @Override
  public String toString() {
    return "zk@" + mZkAddress;
  }
}
