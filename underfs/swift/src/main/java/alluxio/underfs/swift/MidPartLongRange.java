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

package alluxio.underfs.swift;

import org.javaswift.joss.headers.object.range.AbstractRange;

/**
 * A range in a Swift object. This class is a substitute for JOSS MidPartRange which takes 'int'
 * parameters that might overflow for large objects.
 */
public class MidPartLongRange extends AbstractRange {
  /**
   * Constructor for a range in a Swift object.
   *
   * @param startPos starting position in bytes
   * @param endPos ending position in bytes
   */
  public MidPartLongRange(long startPos, long endPos) {
    super(startPos, endPos);
  }

  @Override
  public long getFrom(int byteArrayLength) {
    return this.offset;
  }

  @Override
  public long getTo(int byteArrayLength) {
    return this.length;
  }
}
