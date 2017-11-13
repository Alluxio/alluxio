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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.thrift.UnmountTOptions;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for unmounting a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "@id")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class UnmountOptions extends CommonOptions<UnmountOptions> {
  /**
   * @return the default {@link UnmountOptions}
   */
  public static UnmountOptions defaults() {
    return new UnmountOptions();
  }

  private UnmountOptions() {
    // No options currently
  }

  @Override
  public UnmountOptions getThis() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnmountOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return toStringHelper().toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public UnmountTOptions toThrift() {
    UnmountTOptions options = new UnmountTOptions();
    options.setCommonOptions(commonThrift());
    return options;
  }
}
