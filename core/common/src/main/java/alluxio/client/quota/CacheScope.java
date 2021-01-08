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

package alluxio.client.quota;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class defines "Scope" of a cache quota. For Presto caching, scope is
 * hierarchical with different levels including:
 * <ul>
 *   <li>GLOBAL (".") for global scope;</li>
 *   <li>SCHEMA (e.g., "schema1") for an entire schema;</li>
 *   <li>TABLE (e.g., "schema2.table1") for a given table;</li>
 *   <li>PARTITION (e.g., "schema3.table2.partition1") for a given partition</li>
 * </ul>
 */
@ThreadSafe
public class CacheScope {
  private static final String GLOBAL_ID = ".";
  public static final String SEPARATOR = ".";
  public static final CacheScope GLOBAL = new CacheScope(GLOBAL_ID, 1, Level.GLOBAL);

  /**
   * Level of a scope.
   */
  public enum Level {
    GLOBAL("\\."),
    SCHEMA("\\w+"),
    TABLE("\\w+\\.\\w+"),
    PARTITION("\\w+\\.\\w+.\\w+");

    private final Pattern mPattern;

    /**
     * @param re regexp of the scope
     */
    Level(String re) {
      mPattern = Pattern.compile(re);
    }

    /**
     * @return parent level
     */
    @Nullable
    public Level parent() {
      if (ordinal() > 0) {
        return values()[ordinal() - 1];
      } else {
        return null;
      }
    }

    /**
     * @param input the input property key string
     * @return whether the input string matches this template
     */
    public boolean matches(String input) {
      Matcher matcher = mPattern.matcher(input);
      return matcher.matches();
    }
  }

  private final String mId;
  private final int mLength;
  private final Level mLevel;

  /**
   * @param id id of the scope
   * @return an instance of this scope converted from a string
   */
  public static CacheScope create(String id) {
    Preconditions.checkArgument(id != null && id.length() > 0,
        "scope id can not be null or empty string");
    if (GLOBAL_ID.equals(id)) {
      return GLOBAL;
    } else if (Level.SCHEMA.matches(id)) {
      return new CacheScope(id, id.length(), Level.SCHEMA);
    } else if (Level.TABLE.matches(id)) {
      return new CacheScope(id, id.length(), Level.TABLE);
    } else if (Level.PARTITION.matches(id)) {
      return new CacheScope(id, id.length(), Level.PARTITION);
    }
    throw new IllegalArgumentException("Failed to parse cache scope: " + id);
  }

  private CacheScope(String id, int len, Level level) {
    mId = id;
    mLength = len;
    mLevel = level;
  }

  /**
   * @return parent of this scope
   */
  @Nullable
  public CacheScope parent() {
    int r = mId.lastIndexOf(SEPARATOR, mLength - 1);
    if (r < 0) {
      return GLOBAL;
    } else if (r == 0) {
      return null;
    }
    return new CacheScope(mId, r, mLevel.parent());
  }

  /**
   * @return the level of this scope
   */
  public Level level() {
    return mLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CacheScope cacheScope = (CacheScope) o;
    if (mLength != cacheScope.mLength) {
      return false;
    }
    if (Objects.equal(mId, cacheScope.mId)) {
      return true;
    }
    return Objects.equal(
          mId.substring(0, mLength), cacheScope.mId.substring(0, cacheScope.mLength));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId.substring(0, mLength), mLength);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", mId.substring(0, mLength))
        .toString();
  }
}
