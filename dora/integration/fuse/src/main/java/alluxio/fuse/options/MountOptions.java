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

package alluxio.fuse.options;

import alluxio.conf.PropertyKey;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Options for an Alluxio Fuse mount point.
 */
public class MountOptions {

  protected static final String FUSE_VERSION_OPTION_NAME = "fuse";
  protected static final String DATA_CACHE_DIRS_OPTION_NAME = "local_data_cache";
  protected static final String DATA_CACHE_SIZES_OPTION_NAME = "local_cache_size";
  protected static final String METADATA_CACHE_SIZE_OPTION_NAME = "local_metadata_cache_size";
  protected static final String METADATA_CACHE_EXPIRE_TIME_OPTION_NAME =
      "local_metadata_cache_expire";

  protected final Map<String, String> mMountOptions;

  /**
   * Constructor.
   *
   * @param optionsMap a map contains option keys and values
   */
  public MountOptions(Map<String, String> optionsMap) {
    mMountOptions = optionsMap;
  }

  /**
   * A set recognized option keys. Note inheriting classes may define their own set of keys.
   */
  protected Set<String> getAlluxioFuseSpecificMountOptionKeys() {
    return ImmutableSet.of(
        FUSE_VERSION_OPTION_NAME,
        DATA_CACHE_DIRS_OPTION_NAME,
        DATA_CACHE_SIZES_OPTION_NAME,
        METADATA_CACHE_SIZE_OPTION_NAME,
        METADATA_CACHE_EXPIRE_TIME_OPTION_NAME
    );
  }

  /**
   * @return fuse version
   * @see alluxio.jnifuse.utils.LibfuseVersion
   */
  public Optional<String> getFuseVersion() {
    return Optional.ofNullable(mMountOptions.get(FUSE_VERSION_OPTION_NAME));
  }

  /**
   * @return list of data cache directories
   */
  public Optional<String> getDataCacheDirs() {
    return Optional.ofNullable(mMountOptions.get(DATA_CACHE_DIRS_OPTION_NAME));
  }

  /**
   * @return list of capacities of data cache directories
   */
  public Optional<String> getDataCacheSizes() {
    return Optional.ofNullable(mMountOptions.get(DATA_CACHE_SIZES_OPTION_NAME));
  }

  /**
   * @return size of metadata cache
   */
  public Optional<String> getMetadataCacheSize() {
    return Optional.ofNullable(mMountOptions.get(METADATA_CACHE_SIZE_OPTION_NAME));
  }

  /**
   * @return expiration time of metadata entries
   */
  public Optional<String> getMetadataCacheExpireTime() {
    return Optional.ofNullable(mMountOptions.get(METADATA_CACHE_EXPIRE_TIME_OPTION_NAME));
  }

  /**
   * @return map of recognized alluxio properties and values not specific to Alluxio Fuse
   */
  public Map<String, String> getAlluxioOptions() {
    return Maps.filterKeys(mMountOptions, PropertyKey::isValid);
  }

  /**
   * Gets options that are neither an Alluxio property, nor an option special to Alluxio Fuse.
   * Typically, these are libfuse mount options like {@code ro} and {@code noatime}, but
   * also include invalid options supplied by the user.
   *
   * @return options unrecognized by Alluxio Fuse
   */
  public Map<String, String> getUnrecognizedOptions() {
    Set<String> alluxioSepecificKeys = getAlluxioFuseSpecificMountOptionKeys();
    return Maps.filterKeys(mMountOptions, (key) -> {
      return !alluxioSepecificKeys.contains(key) && !PropertyKey.isValid(key);
    });
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MountOptions that = (MountOptions) o;
    return Objects.equals(mMountOptions, that.mMountOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mMountOptions);
  }
}
