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

package alluxio.client.lineage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.lineage.options.CreateLineageOptions;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.client.lineage.options.GetLineageInfoListOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.AlluxioException;
import alluxio.exception.ConnectionFailedException;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.wire.LineageInfo;

/**
 * Tachyon Lineage client. This class provides implementation of interacting with Tachyon Lineage
 * master.
 */
@PublicApi
@ThreadSafe
public abstract class AbstractLineageClient implements LineageClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected LineageContext mContext;

  /**
   * Constructs a new instance with a {@link LineageContext}.
   */
  public AbstractLineageClient() {
    mContext = LineageContext.INSTANCE;
  }

  @Override
  public long createLineage(List<AlluxioURI> inputFiles, List<AlluxioURI> outputFiles, Job job,
      CreateLineageOptions options) throws FileDoesNotExistException, AlluxioException,
      IOException {
    // TODO(yupeng): relax this to support other type of jobs
    Preconditions.checkState(job instanceof CommandLineJob,
        PreconditionMessage.COMMAND_LINE_LINEAGE_ONLY);
    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long lineageId = masterClient.createLineage(stripURIList(inputFiles),
          stripURIList(outputFiles), (CommandLineJob) job);
      LOG.info("Created lineage {}", lineageId);
      return lineageId;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean deleteLineage(long lineageId, DeleteLineageOptions options)
      throws IOException, LineageDoesNotExistException, LineageDeletionException, AlluxioException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.deleteLineage(lineageId, options.isCascade());
      LOG.info("{} delete lineage {}", result ? "Succeeded to " : "Failed to ", lineageId);
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<LineageInfo> getLineageInfoList(GetLineageInfoListOptions options)
      throws IOException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();

    try {
      return masterClient.getLineageInfoList();
    } catch (ConnectionFailedException e) {
      throw new IOException(e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Transforms the list of {@link AlluxioURI} in a new list of Strings,
   * where each string is {@link AlluxioURI#getPath()}
   * @param uris the list of {@link AlluxioURI}s to be stripped
   * @return a new list of strings mapping the input URIs to theri path component
   */
  private List<String> stripURIList(List<AlluxioURI> uris) {
    final List<String> pathStrings = new ArrayList<String>(uris.size());
    for (final AlluxioURI uri : uris) {
      pathStrings.add(uri.getPath());
    }
    return pathStrings;
  }
}
