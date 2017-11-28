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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.underfs.options.FileLocationOptions;

import java.util.List;

/**
 * Cache for block locations in the UFS.
 */
public interface UfsBlockLocationCache {
  /**
   * Retrieves the block locations from UFS, and caches the result.
   * If failed to get the locations from UFS, an empty list is returned and nothing is cached.
   * The result will overwrite the existing cached locations for the block.
   *
   * @param blockId the block ID
   * @param fileUri the URI of the file which contains the block
   * @param options the options for getting file locations from UFS
   * @return the block locations in UFS
   * @throws InvalidPathException if the fileUri is an invalid path
   */
  List<String> process(long blockId, AlluxioURI fileUri, FileLocationOptions options)
      throws InvalidPathException;

  /**
   * @param blockId the block ID
   * @return the cached block locations or null if there is no cached locations for the block
   */
  List<String> get(long blockId);

  /**
   * Invalidates the UFS locations for the block.
   *
   * @param blockId the block ID
   */
  void invalidate(long blockId);

  /**
   * Factory class for {@link UfsBlockLocationCache}.
   */
  final class Factory {
    private Factory() {} // prevent instantiation

    public static UfsBlockLocationCache create(MountTable mountTable) {
      return new LazyUfsBlockLocationCache(mountTable);
    }
  }
}
