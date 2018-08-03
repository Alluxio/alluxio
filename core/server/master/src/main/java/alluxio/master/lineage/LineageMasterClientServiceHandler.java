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

package alluxio.master.lineage;

import alluxio.uri.AlluxioURI;
import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AlluxioException;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CommandLineJobInfo;
import alluxio.thrift.CreateLineageTOptions;
import alluxio.thrift.CreateLineageTResponse;
import alluxio.thrift.DeleteLineageTOptions;
import alluxio.thrift.DeleteLineageTResponse;
import alluxio.thrift.GetLineageInfoListTOptions;
import alluxio.thrift.GetLineageInfoListTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.LineageInfo;
import alluxio.thrift.LineageMasterClientService;
import alluxio.thrift.ReinitializeFileTOptions;
import alluxio.thrift.ReinitializeFileTResponse;
import alluxio.thrift.ReportLostFileTOptions;
import alluxio.thrift.ReportLostFileTResponse;
import alluxio.thrift.TTtlAction;
import alluxio.wire.TtlAction;

import com.google.common.base.Preconditions;

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
  LineageMasterClientServiceHandler(LineageMaster lineageMaster) {
    Preconditions.checkNotNull(lineageMaster, "lineageMaster");
    mLineageMaster = lineageMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public CreateLineageTResponse createLineage(final List<String> inputFiles,
      final List<String> outputFiles, final CommandLineJobInfo jobInfo,
      CreateLineageTOptions options) throws AlluxioTException {
    return RpcUtils.call(new RpcCallableThrowsIOException<CreateLineageTResponse>() {
      @Override
      public CreateLineageTResponse call() throws AlluxioException, IOException {
        // deserialization
        List<AlluxioURI> inputFilesUri = new ArrayList<>();
        for (String inputFile : inputFiles) {
          inputFilesUri.add(new AlluxioURI(inputFile));
        }
        List<AlluxioURI> outputFilesUri = new ArrayList<>();
        for (String outputFile : outputFiles) {
          outputFilesUri.add(new AlluxioURI(outputFile));
        }
        CommandLineJob job = new CommandLineJob(jobInfo.getCommand(),
            new JobConf(jobInfo.getConf().getOutputFile()));
        return new CreateLineageTResponse(
            mLineageMaster.createLineage(inputFilesUri, outputFilesUri, job));
      }
    });
  }

  @Override
  public DeleteLineageTResponse deleteLineage(final long lineageId, final boolean cascade,
      DeleteLineageTOptions options) throws AlluxioTException {
    return RpcUtils.call(new RpcCallableThrowsIOException<DeleteLineageTResponse>() {
      @Override
      public DeleteLineageTResponse call() throws AlluxioException, IOException {
        return new DeleteLineageTResponse(mLineageMaster.deleteLineage(lineageId, cascade));
      }
    });
  }

  @Override
  public GetLineageInfoListTResponse getLineageInfoList(GetLineageInfoListTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(new RpcCallable<GetLineageInfoListTResponse>() {
      @Override
      public GetLineageInfoListTResponse call() throws AlluxioException {
        List<LineageInfo> result = new ArrayList<>();
        for (alluxio.wire.LineageInfo lineageInfo : mLineageMaster.getLineageInfoList()) {
          result.add(lineageInfo.toThrift());
        }
        return new GetLineageInfoListTResponse(result);
      }
    });
  }

  @Override
  public ReinitializeFileTResponse reinitializeFile(final String path, final long blockSizeBytes,
      final long ttl, final TTtlAction ttlAction, ReinitializeFileTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(
    (RpcCallableThrowsIOException<ReinitializeFileTResponse>) () ->
    new ReinitializeFileTResponse(mLineageMaster
        .reinitializeFile(path, blockSizeBytes, ttl, TtlAction.fromThrift(ttlAction))));
  }

  @Override
  public ReportLostFileTResponse reportLostFile(final String path, ReportLostFileTOptions options)
      throws AlluxioTException {
    return RpcUtils.call((RpcCallableThrowsIOException<ReportLostFileTResponse>) () -> {
      mLineageMaster.reportLostFile(path);
      return new ReportLostFileTResponse();
    });
  }
}
