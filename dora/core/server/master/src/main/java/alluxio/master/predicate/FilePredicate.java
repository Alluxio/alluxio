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

package alluxio.master.predicate;

import alluxio.proto.journal.Job.FileFilter;
import alluxio.wire.FileInfo;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;

/**
 * A FilePredicate represents an expression which evaluates to true or false for a file filter.
 */
public interface FilePredicate {
  LoadingCache<ClassLoader, List<FilePredicateFactory>> ALL_FACTORIES =
      CacheBuilder.newBuilder().maximumSize(100)
          .build(new CacheLoader<ClassLoader, List<FilePredicateFactory>>() {
            public List<FilePredicateFactory> load(ClassLoader key) throws Exception {
              ServiceLoader<FilePredicateFactory> serviceLoader =
                  ServiceLoader.load(FilePredicateFactory.class, key);
              List<FilePredicateFactory> factories = new ArrayList<>();
              for (FilePredicateFactory factory : serviceLoader) {
                factories.add(factory);
              }
              return factories;
            }
          });

  /**
   * Get the predicate function from the file predicate.
   * @return the predicate function
   */
  Predicate<FileInfo> get();

  /**
   * Creates a file predicate from a file filter.
   * If the filter name is invalid, it will throw exception.
   *
   * @param filter the file filter
   * @return the file predicate
   */
  static FilePredicate create(FileFilter filter) {
    Objects.requireNonNull(filter);
    List<FilePredicateFactory> factories;
    try {
      factories = ALL_FACTORIES.get(FilePredicateFactory.class.getClassLoader());
    } catch (ExecutionException e) {
      throw new UnsupportedOperationException(
          "No custom predicate factories available: " + e.getCause());
    }

    for (FilePredicateFactory factory : factories) {
      FilePredicate predicate = factory.create(filter);
      if (predicate != null) {
        return predicate;
      }
    }
    throw new UnsupportedOperationException("Invalid filter name: " + filter.getName());
  }
}
