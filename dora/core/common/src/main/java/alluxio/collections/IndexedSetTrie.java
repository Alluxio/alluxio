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

package alluxio.collections;

import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A set of objects that are indexed and thus can be queried by specific fields of the object.
 * This class extends {@link IndexedSet} and adds support for prefix-based searching using a trie
 * for non-unique fields.
 *
 * @param <T> the type of object
 */
@ThreadSafe
public class IndexedSetTrie<T> extends IndexedSet<T> {

  /**
   * Constructs a new {@link IndexedSetTrie} instance with at least one field as the index.
   *
   * @param primaryIndexDefinition at least one field is needed to index the set of objects. This
   *        primaryIndexDefinition is used to initialize {@link IndexedSet#mPrimaryIndex} and is
   *        recommended to be unique in consideration of performance.
   * @param FileIdIndexDefinition the index definition for the field to be used as the file id
   * @param otherIndexDefinitions other index definitions to index the set
   */
  @SafeVarargs
  public IndexedSetTrie(IndexDefinition<T, ?> primaryIndexDefinition,
                        IndexDefinition<T, ?> FileIdIndexDefinition,
                        IndexDefinition<T, ?>... otherIndexDefinitions) {
    super(primaryIndexDefinition, otherIndexDefinitions);

    FieldIndex<T, ?> fileIdIndex = new NonUniqueFieldIndexInTrie<>(FileIdIndexDefinition);
    mIndices.put(FileIdIndexDefinition, fileIdIndex);
  }

  /**
   * Gets a subset of objects with the specified prefix of the index field value. If there is no
   * object with the specified prefix, a newly created empty set is returned.
   *
   * @param indexDefinition the field index definition
   * @param prefix the prefix of the field value to be satisfied
   * @param <V> the field type
   * @return the set of objects or an empty set if no such object exists
   */
  public <V> Set<T> getByPrefix(IndexDefinition<T, V> indexDefinition, String prefix) {
    FieldIndex<T, V> index = (FieldIndex<T, V>) mIndices.get(indexDefinition);
    if (index == null) {
      throw new IllegalStateException("The given index isn't defined for this IndexedSet");
    }

    // Check if the index supports prefix search
    if (index instanceof NonUniqueFieldIndexInTrie) {
      return ((NonUniqueFieldIndexInTrie<T, V>) index).getByPrefix(prefix);
    } else {
      throw new UnsupportedOperationException(
        "Prefix search is only supported for NonUniqueFieldIndexInTrie");
    }
  }
}
