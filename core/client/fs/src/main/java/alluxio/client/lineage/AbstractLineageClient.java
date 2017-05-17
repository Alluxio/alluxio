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

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.lineage.options.CreateLineageOptions;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.client.lineage.options.GetLineageInfoListOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.job.CommandLineJob;
import alluxio.job.Job;
import alluxio.wire.LineageInfo;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio Lineage client. This class provides implementation of interacting with Alluxio Lineage
 * master.
 */
@PublicApi
@ThreadSafe
public abstract class AbstractLineageClient implements LineageClient {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractLineageClient.class);

  protected LineageContext mContext;

  /**
   * Constructs a new instance with a {@link LineageContext}.
   *
   * @param context lineage context
   */
  public AbstractLineageClient(LineageContext context) {
    mContext = context;
  }

  @Override
  public long createLineage(List<AlluxioURI> inputFiles, List<AlluxioURI> outputFiles, Job job,
      CreateLineageOptions options) throws FileDoesNotExistException, AlluxioException,
      IOException {
    // TODO(yupeng): relax this to support other type of jobs
    Preconditions.checkState(job instanceof CommandLineJob,
        PreconditionMessage.COMMAND_LINE_LINEAGE_ONLY);
    LineageMasterClient masterClient = null;
    try {
      masterClient = mContext.acquireMasterClient();
      long lineageId = masterClient.createLineage(stripURIList(inputFiles),
          stripURIList(outputFiles), (CommandLineJob) job);
      LOG.info("Created lineage {}", lineageId);
      return lineageId;
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean deleteLineage(long lineageId, DeleteLineageOptions options)
      throws IOException, LineageDoesNotExistException, LineageDeletionException, AlluxioException {
    LineageMasterClient masterClient = null;
    try {
      masterClient = mContext.acquireMasterClient();
      boolean result = masterClient.deleteLineage(lineageId, options.isCascade());
      LOG.info("{} delete lineage {}", result ? "Succeeded to " : "Failed to ", lineageId);
      return result;
    } catch (NotFoundException e) {
      throw new LineageDoesNotExistException(e.getMessage());
    } catch (FailedPreconditionException e) {
      throw new LineageDeletionException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<LineageInfo> getLineageInfoList(GetLineageInfoListOptions options)
      throws IOException {
    LineageMasterClient masterClient = null;
    try {
      masterClient = mContext.acquireMasterClient();
      return masterClient.getLineageInfoList();
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Transforms the list of {@link AlluxioURI} in a new list of Strings,
   * where each string is {@link AlluxioURI#getPath()}.
   *
   * @param uris the list of {@link AlluxioURI}s to be stripped
   * @return a new list of strings mapping the input URIs to their path component
   */
  private List<String> stripURIList(List<AlluxioURI> uris) {
    final List<String> pathStrings = new ArrayList<>(uris.size());
    for (final AlluxioURI uri : uris) {
      pathStrings.add(uri.getPath());
    }
    return pathStrings;
  }
}
