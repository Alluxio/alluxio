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

package tachyon.master.lineage;

import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.exception.TachyonException;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageMasterWorkerService;
import tachyon.thrift.TachyonTException;

/**
 * This class is a Thrift handler for lineage master RPCs invoked by a Tachyon worker.
 */
public final class LineageMasterWorkerServiceHandler implements LineageMasterWorkerService.Iface {
  private final LineageMaster mLineageMaster;

  /**
   * Creates a new instance of {@link LineageMasterWorkerServiceHandler}.
   *
   * @param lineageMaster the {@link LineageMaster} the handler uses internally
   */
  public LineageMasterWorkerServiceHandler(LineageMaster lineageMaster) {
    Preconditions.checkNotNull(lineageMaster);
    mLineageMaster = lineageMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.LINEAGE_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public LineageCommand heartbeat(long workerId, List<Long> persistedFiles)
      throws TachyonTException {
    try {
      return mLineageMaster.lineageWorkerHeartbeat(workerId, persistedFiles);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }
}
