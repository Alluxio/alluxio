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

package tachyon.client.lineage;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientOptions;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFileSystem;
import tachyon.job.Job;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Tachyon lineage client. This class is the entry point for all lineage related operations. An
 * instance of this class can be obtained via {@link TachyonLineageFileSystem#get}.This class is thread safe.
 */
@PublicApi
public class TachyonLineageFileSystem extends TachyonFileSystem{
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  protected LineageContext mContext;

  protected TachyonLineageFileSystem() {
    super();
    mContext = LineageContext.INSTANCE;
  }

  /**
   * Creates and adds a lineage. It requires all the input files must either exist in Tachyon
   * storage, or have been added as output files in other lineages. It also requires the output
   * files do not exist in Tachyon, and it will create an empty file for each of it.
   *
   * @param inputFiles the files that the job depends on
   * @param outputFiles the files that the job outputs
   * @param job the job to track
   * @return the dependency id
   * @throws IOException
   * @throws FileDoesNotExistException
   */
  public long addLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job)
      throws FileDoesNotExistException, IOException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();

    try {
      long lineageId = masterClient.addLineage(inputFiles, outputFiles, job);
      LOG.info("Added lineage "+lineageId);
      return lineageId;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Deletes a lineage identified by a given id. If the delete is cascade, it will delete all the
   * downstream lineages that depend on the given one recursively. Otherwise it does not delete the
   * lineage exception, if there are other lineages whose input files are the output files of the
   * specified lineage.
   *
   * @param lineageId the id of the lineage
   * @param cascade whether to delete all the downstream lineages recursively
   * @return true if the lineage deletion is successful, false otherwise
   * @throws IOException
   */
  public boolean deleteLineage(long lineageId, boolean cascade) throws IOException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();

    try {
      boolean result = masterClient.deleteLineage(lineageId, cascade);
      LOG.info(result ? "Succeeded to " : "Failed to" + "add lineage " + lineageId);
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * All the files are already created when lineage was added. This method reconfigures the created
   * file.
   *
   * @throws IOException
   */
  @Override
  public long create(TachyonURI path, long blockSize, boolean recursive) {
    LineageMasterClient masterClient = mContext.acquireMasterClient();

    try {
      long fileId = masterClient.recreateFile(path.getPath(), blockSize);
      LOG.info("Recreated file " + path + " with blockSize: " + blockSize);
      return fileId;
    } catch (IOException e) {
      // TODO(yupeng): error handling.
      throw new RuntimeException("recreation failed", e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileOutStream getOutStream(TachyonURI path, ClientOptions options) throws IOException,
      InvalidPathException, FileAlreadyExistException, BlockInfoException {
    long fileId = create(path, options.getBlockSize(), true);
    return new LineageFileOutStream(fileId, options);
  }
}
