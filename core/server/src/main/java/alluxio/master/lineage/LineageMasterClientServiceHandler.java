/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.exception.AlluxioException;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CommandLineJobInfo;
import alluxio.thrift.LineageInfo;
import alluxio.thrift.LineageMasterClientService;
import alluxio.thrift.ThriftIOException;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for lineage master RPCs invoked by an Alluxio client.
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
      CommandLineJobInfo jobInfo) throws AlluxioTException, ThriftIOException {
    // deserialization
    List<AlluxioURI> inputFilesUri = Lists.newArrayList();
    for (String inputFile : inputFiles) {
      inputFilesUri.add(new AlluxioURI(inputFile));
    }
    List<AlluxioURI> outputFilesUri = Lists.newArrayList();
    for (String outputFile : outputFiles) {
      outputFilesUri.add(new AlluxioURI(outputFile));
    }

    CommandLineJob job =
        new CommandLineJob(jobInfo.getCommand(), new JobConf(jobInfo.getConf().getOutputFile()));
    try {
      return mLineageMaster.createLineage(inputFilesUri, outputFilesUri, job);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    } catch (IOException e) {
      throw new ThriftIOException(e.getMessage());
    }
  }

  @Override
  public boolean deleteLineage(long lineageId, boolean cascade) throws AlluxioTException {
    try {
      return mLineageMaster.deleteLineage(lineageId, cascade);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public long reinitializeFile(String path, long blockSizeBytes, long ttl)
      throws AlluxioTException {
    try {
      return mLineageMaster.reinitializeFile(path, blockSizeBytes, ttl);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public void reportLostFile(String path) throws AlluxioTException, ThriftIOException {
    try {
      mLineageMaster.reportLostFile(path);
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public List<LineageInfo> getLineageInfoList() throws AlluxioTException {
    try {
      List<LineageInfo> result = new ArrayList<LineageInfo>();
      for (alluxio.wire.LineageInfo lineageInfo : mLineageMaster.getLineageInfoList()) {
        result.add(ThriftUtils.toThrift(lineageInfo));
      }
      return result;
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }
}
