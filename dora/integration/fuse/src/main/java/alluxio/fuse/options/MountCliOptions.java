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

import alluxio.collections.Pair;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.ratis.thirdparty.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Mount options specified with {@code -o} from the command line.
 */
public class MountCliOptions {
  @Parameter(
      names = {"-o"},
      description = "Providing mount options separating by comma. "
          + "Mount options includes operating system mount options, "
          + "many FUSE specific mount options (e.g. direct_io,attr_timeout=10s,allow_other), "
          + "Alluxio property key=value pairs, and Alluxio FUSE special mount options "
          + "local_data_cache=<local_cache_directory>,local_cache_size=<size>,"
          + "local_metadata_cache_size=<size>,local_metadata_cache_expire=<timeout>",
      listConverter = KvPairsConverter.class
  )
  // Notes:
  // 1. this is conceptually a Map<String, String>, but due to limitations in JCommander, Map cannot
  //    be used as the type of the field, otherwise it does not allow specifying multiple
  //    occurrences of the `-o` option
  // 2. the list must be a mutable list, e.g. ArrayList. JCommander will try to put multiple
  //    occurrences of the option into the list, instead of instantiating a new one and copying
  //    the elements every time an occurrence is spotted
  protected List<Pair<String, String>> mMountOptions = new ArrayList<>();

  private static class KvPairsConverter
      extends BaseValueConverter<List<Pair<String, String>>> {
    /**
     * Constructor.
     *
     * @param optionName the name of the option that this converter is used for
     */
    public KvPairsConverter(String optionName) {
      super(optionName);
    }

    @Override
    public List<Pair<String, String>> convert(String kvPair) {
      // kvPair is a string that looks like "key=value"
      String[] kv = kvPair.split("=");
      if (kvPair.isEmpty() || kv.length == 0) {
        throw new ParameterException(getErrorString(kvPair, "a `key=value` pair",
            "value is empty"));
      }
      if (kv.length == 1) {
        return Lists.newArrayList(new Pair<>(kv[0], ""));
      } else if (kv.length == 2) {
        return Lists.newArrayList(new Pair<>(kv[0], kv[1]));
      } else {
        throw new ParameterException(getErrorString(kvPair, "a `key=value` pair",
            "contains more than 1 `=`"));
      }
    }
  }

  /**
   * @return mount options
   */
  public MountOptions getMountOptions() {
    Map<String, String> map = getOptionsMap();
    return new MountOptions(map);
  }

  /**
   * @return raw key-value map of mount options
   */
  @VisibleForTesting
  public Map<String, String> getOptionsMap() {
    return mMountOptions.stream()
        .collect(ImmutableMap.toImmutableMap(
            Pair::getFirst,
            Pair::getSecond,
            // merge function: use value of the last occurrence if the key occurs multiple times
            (oldValue, newValue) -> newValue
        ));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MountCliOptions that = (MountCliOptions) o;
    return Objects.equals(mMountOptions, that.mMountOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mMountOptions);
  }
}
