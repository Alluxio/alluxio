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

package alluxio.proxy.s3;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is Range Spec for Amazon S3 API.
 */
public class S3RangeSpec {
  private static final Pattern RANGE_PATTERN = Pattern.compile("^bytes=(\\d*)-(\\d*)$");

  public static final S3RangeSpec INVALID_S3_RANGE_SPEC = new S3RangeSpec(false);

  /* Does the range spec is valid */
  private final boolean mIsValid;
  /* Start offset specified in range spec */
  private final long mStart;
  /* End offset specified in range spec */
  private final long mEnd;

  /**
   * @param isValid the spec is valid or not
   */
  protected S3RangeSpec(boolean isValid) {
    mIsValid = isValid;
    mStart = -1;
    mEnd = -1;
  }

  /**
   * @param start          the start offset of object
   * @param end            the end offset of object
   */
  public S3RangeSpec(long start, long end) {
    mIsValid = true;
    mStart = start;
    mEnd = end;
  }

  /**
   * Get real length of object.
   *
   * @param objectSize the object size
   * @return the real object length referring to range
   */
  public long getLength(long objectSize) {
    if (!mIsValid) {
      return objectSize;
    }

    long length;
    if (isSuffixLength()) {
      length = mEnd;
      if (length > objectSize) {
        length = objectSize;
      }
    } else {
      // NOTE: when start larger than object size, return 0
      if (objectSize <= mStart) {
        length = 0;
      } else if (mEnd == -1 || objectSize <= mEnd) {
        length = objectSize - mStart;
      } else {
        length = mEnd - mStart + 1;
      }
    }
    return length;
  }

  /**
   * Get real offset of object.
   *
   * @param objectSize the object size
   * @return the real object offset referring to range
   */
  public long getOffset(long objectSize) {
    if (!mIsValid) {
      return 0;
    }

    long start = mStart;
    if (isSuffixLength()) {
      start = objectSize - mEnd;
      if (start < 0) {
        start = 0;
      }
    } else {
      if (start >= objectSize) {
        start = 0;
      }
    }
    return start;
  }

  private boolean isSuffixLength() {
    return mStart == -1;
  }

  /**
   * Factory for {@link S3RangeSpec}.
   */
  public static final class Factory {
    /**
     * Create {@link S3RangeSpec} from http range header.
     *
     * @param range the http range header
     * @return the {@link S3RangeSpec}
     */
    public static S3RangeSpec create(final String range) {
      if (StringUtils.isEmpty(range)) {
        return INVALID_S3_RANGE_SPEC;
      }

      Matcher matcher = RANGE_PATTERN.matcher(range);
      if (!matcher.matches()) {
        return INVALID_S3_RANGE_SPEC;
      }

      String startStr = matcher.group(1);
      long start = -1;
      if (!StringUtils.isEmpty(startStr)) {
        start = Long.parseLong(startStr);
      }
      String endStr = matcher.group(2);
      long end = -1;
      if (!StringUtils.isEmpty(endStr)) {
        end = Long.parseLong(endStr);
      }

      if (start > -1 && end > -1) {
        if (start > end) {
          return INVALID_S3_RANGE_SPEC;
        }
        return new S3RangeSpec(start, end);
      } else if (start > -1) {
        return new S3RangeSpec(start, -1);
      } else if (end > -1) {
        if (end == 0) {
          return INVALID_S3_RANGE_SPEC;
        }
        return new S3RangeSpec(-1, end);
      } else {
        return INVALID_S3_RANGE_SPEC;
      }
    }
  }
}
