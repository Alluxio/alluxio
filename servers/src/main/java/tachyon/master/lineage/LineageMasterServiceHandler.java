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

import tachyon.TachyonURI;
import tachyon.job.Job;
import tachyon.thrift.LineageCommand;
import tachyon.thrift.LineageMasterService.Iface;
import tachyon.util.io.SerDeUtils;

public final class LineageMasterServiceHandler implements Iface {
  private final LineageMaster mLineageMaster;

  public LineageMasterServiceHandler(LineageMaster lineageMaster) {
    mLineageMaster = lineageMaster;
  }

  @Override
  public long createLineage(List<String> inputFiles, List<String> outputFiles, ByteBuffer job)
      throws TException {
    // deserialization
    List<TachyonURI> inputFilesUri = Lists.newArrayList();
    for (String inputFile : inputFiles) {
      inputFilesUri.add(new TachyonURI(inputFile));
    }
    List<TachyonURI> outputFilesUri = Lists.newArrayList();
    for (String output : outputFiles) {
      outputFilesUri.add(new TachyonURI(output));
    }
    return mLineageMaster.createLineage(inputFilesUri, outputFilesUri,
        (Job) SerDeUtils.byteArrayToObject(job.array()));
  }

  @Override
  public boolean deleteLineage(long lineageId, boolean cascade) throws TException {
    return mLineageMaster.deleteLineage(lineageId, cascade);
  }

  @Override
  public LineageCommand workerLineageHeartbeat(long workerId) throws TException {
    return mLineageMaster.lineageWorkerHeartbeat(workerId);
  }

  @Override
  public void asyncCompleteFile(long fileId, String filePath) throws TException {
    mLineageMaster.asyncCompleteFile(fileId, filePath);
  }

  @Override
  public long recreateFile(String path, long blockSizeBytes) throws TException {
    return mLineageMaster.recreateFile(path, blockSizeBytes);
  }

}
