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
import alluxio.thrift.RenameTOptions;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for renaming a file or a directory.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class, property = "@id")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class RenameOptions extends CommonOptions<RenameOptions> {
  /**
   * @return the default {@link RenameOptions}
   */
  public static RenameOptions defaults() {
    return new RenameOptions();
  }

  private RenameOptions() {
    // No options currently
  }

  @Override
  public RenameOptions getThis() {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RenameOptions)) {
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
  public RenameTOptions toThrift() {
    RenameTOptions options = new RenameTOptions();
    options.setCommonOptions(commonThrift());
    return options;
  }
}
