/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file.policy;

import java.util.List;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.block.BlockWorkerInfo;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Interface for the location policy of which workers a file's block are written into. A file policy
 * instance is used only once per file write, and only when the hostname in the options is not
 * specified.
 */
@PublicApi
public interface FileWriteLocationPolicy {
  class Factory {
    /**
     * Creates the policy as specified in Tachyon configuration. The policy is constructed with the
     * information of the active workers, and the the options of the file output stream.
     *
     * @param conf {@link TachyonConf} to determine the policy
     * @param options the options of the file output stream
     * @return the generated policy
     */
    public static FileWriteLocationPolicy createPolicy(TachyonConf conf, OutStreamOptions options) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<FileWriteLocationPolicy>getClass(Constants.USER_FILE_WRITE_LOCATION_POLICY),
            new Class[] {List.class, OutStreamOptions.class},
            new Object[] {TachyonBlockStore.get().getBlockWorkerInfoList(), options});
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Gets the worker's host name for the next block to write to.
   *
   * @param workerInfoList the info of the active workers
   * @return the host name of worker to write to
   */
  public String getWorkerForNextBlock(List<BlockWorkerInfo> workerInfoList);
}
