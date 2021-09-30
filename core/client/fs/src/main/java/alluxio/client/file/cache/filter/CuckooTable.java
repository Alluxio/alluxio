/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache.filter;

public interface CuckooTable {
  public int readTag(int i, int j);

  public void writeTag(int i, int j, int t);

  public boolean findTagInBucket(int i, int tag);

  public boolean findTagInBuckets(int i1, int i2, int tag);

  public boolean deleteTagFromBucket(int i, int tag);

  public int insertOrKickoutOne(int i, int tag);

  public boolean findTagInBucket(int i, int tag, TagPosition position);

  public boolean findTagInBuckets(int i1, int i2, int tag, TagPosition position);

  public boolean deleteTagFromBucket(int i, int tag, TagPosition position);

  public int insertOrKickoutOne(int i, int tag, TagPosition position);

  public boolean insert(int i, int tag, TagPosition position);

  public int numTagsPerBuckets();

  public int numBuckets();

  public int bitsPerTag();

  public int sizeInBytes();

  public int sizeInTags();
}
