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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getting the status of a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class GetStatusOptions extends alluxio.file.options.GetStatusOptions {
  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
<<<<<<< HEAD
    super();
    mCommonOptions = CommonOptions.defaults();
||||||| merged common ancestors
    mCommonOptions = CommonOptions.defaults();
=======
    mCommonOptions = CommonOptions.defaults()
        .setTtl(Configuration.getMs(PropertyKey.USER_FILE_LOAD_TTL))
        .setTtlAction(Configuration.getEnum(PropertyKey.USER_FILE_LOAD_TTL_ACTION,
            TtlAction.class));
>>>>>>> master
    mLoadMetadataType =
        Configuration.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class);
  }
}
