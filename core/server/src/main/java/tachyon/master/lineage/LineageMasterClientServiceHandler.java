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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;
import tachyon.thrift.CommandLineJobInfo;
import tachyon.thrift.LineageInfo;
import tachyon.thrift.LineageMasterClientService;
import tachyon.thrift.TachyonTException;
import tachyon.thrift.ThriftIOException;
import tachyon.wire.ThriftUtils;

/**
 * This class is a Thrift handler for lineage master RPCs invoked by a Tachyon client.
 */
@ThreadSafe
public final class LineageMasterClientServiceHandler implements LineageMasterClientService.Iface {
  private final LineageMaster mLineageMaster;

  /**
   * Creates a new instance of {@link LineageMasterClientServiceHandler}.
   *
   * @param lineageMaster the {@link LineageMaster} the handler uses internally
   */
  public LineageMasterClientServiceHandler(LineageMaster lineageMaster) {
    Preconditions.checkNotNull(lineageMaster);
    mLineageMaster = lineageMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public long createLineage(List<String> inputFiles, List<String> outputFiles,
      CommandLineJobInfo jobInfo) throws TachyonTException, ThriftIOException {
    // deserialization
    List<TachyonURI> inputFilesUri = Lists.newArrayList();
    for (String inputFile : inputFiles) {
      inputFilesUri.add(new TachyonURI(inputFile));
    }
    List<TachyonURI> outputFilesUri = Lists.newArrayList();
    for (String outputFile : outputFiles) {
      outputFilesUri.add(new TachyonURI(outputFile));
    }

    CommandLineJob job =
        new CommandLineJob(jobInfo.getCommand(), new JobConf(jobInfo.getConf().getOutputFile()));
    try {
      return mLineageMaster.createLineage(inputFilesUri, outputFilesUri, job);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public boolean deleteLineage(long lineageId, boolean cascade) throws TachyonTException {
    try {
      return mLineageMaster.deleteLineage(lineageId, cascade);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public long reinitializeFile(String path, long blockSizeBytes, long ttl)
      throws TachyonTException {
    try {
      return mLineageMaster.reinitializeFile(path, blockSizeBytes, ttl);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public void reportLostFile(String path) throws TachyonTException, ThriftIOException {
    try {
      mLineageMaster.reportLostFile(path);
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }

  @Override
  public List<LineageInfo> getLineageInfoList() throws TachyonTException {
    try {
      List<LineageInfo> result = new ArrayList<LineageInfo>();
      for (tachyon.wire.LineageInfo lineageInfo : mLineageMaster.getLineageInfoList()) {
        result.add(ThriftUtils.toThrift(lineageInfo));
      }
      return result;
    } catch (TachyonException e) {
      throw e.toTachyonTException();
    }
  }
}
