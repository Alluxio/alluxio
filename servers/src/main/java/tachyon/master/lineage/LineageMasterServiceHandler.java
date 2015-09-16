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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;

import tachyon.client.file.TachyonFile;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageMasterService.Iface;

public final class LineageMasterServiceHandler implements Iface {
  private final LineageMaster mLineageMaster;

  public LineageMasterServiceHandler(LineageMaster lineageMaster) {
    mLineageMaster = lineageMaster;
  }

  @Override
  public long createLineage(List<Long> inputFiles, List<Long> outputFiles, ByteBuffer job)
      throws TException {
    List<TachyonFile> inputTachyonFiles = Lists.newArrayList();
    List<TachyonFile> outputTachyonFiles = Lists.newArrayList();
    return mLineageMaster.createLineage(inputTachyonFiles, outputTachyonFiles, null);
  }

  @Override
  public boolean deleteLineage(long lineageId) throws TException {
    return mLineageMaster.deleteLineage(lineageId);
  }

  @Override
  public LineageCommand lineageWorkerHeartbeat(long workerId) throws TException {
    return mLineageMaster.lineageWorkerHeartbeat(workerId);
  }

}
