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

package alluxio.master.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RpcUtils;
import alluxio.RpcUtils.RpcCallable;
import alluxio.RpcUtils.RpcCallableThrowsIOException;
import alluxio.exception.AlluxioException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.CompletePartitionTOptions;
import alluxio.thrift.CompletePartitionTResponse;
import alluxio.thrift.CompleteStoreTOptions;
import alluxio.thrift.CompleteStoreTResponse;
import alluxio.thrift.CreateStoreTOptions;
import alluxio.thrift.CreateStoreTResponse;
import alluxio.thrift.DeleteStoreTOptions;
import alluxio.thrift.DeleteStoreTResponse;
import alluxio.thrift.GetPartitionInfoTOptions;
import alluxio.thrift.GetPartitionInfoTResponse;
import alluxio.thrift.GetServiceVersionTOptions;
import alluxio.thrift.GetServiceVersionTResponse;
import alluxio.thrift.KeyValueMasterClientService;
import alluxio.thrift.MergeStoreTOptions;
import alluxio.thrift.MergeStoreTResponse;
import alluxio.thrift.PartitionInfo;
import alluxio.thrift.RenameStoreTOptions;
import alluxio.thrift.RenameStoreTResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class is a Thrift handler for key-value master RPCs invoked by an Alluxio client.
 */
@ThreadSafe
public final class KeyValueMasterClientServiceHandler implements KeyValueMasterClientService.Iface {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyValueMasterClientServiceHandler.class);

  private final KeyValueMaster mKeyValueMaster;

  /**
   * Constructs the service handler to process incoming RPC calls for key-value master.
   *
   * @param keyValueMaster handler to the real {@link KeyValueMaster} instance
   */
  KeyValueMasterClientServiceHandler(KeyValueMaster keyValueMaster) {
    mKeyValueMaster = keyValueMaster;
  }

  @Override
  public GetServiceVersionTResponse getServiceVersion(GetServiceVersionTOptions options) {
    return new GetServiceVersionTResponse(Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION);
  }

  @Override
  public CompletePartitionTResponse completePartition(final String path, final PartitionInfo info,
      CompletePartitionTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<CompletePartitionTResponse>() {
      @Override
      public CompletePartitionTResponse call() throws AlluxioException {
        mKeyValueMaster.completePartition(new AlluxioURI(path), info);
        return new CompletePartitionTResponse();
      }
    });
  }

  @Override
  public CreateStoreTResponse createStore(final String path, CreateStoreTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<CreateStoreTResponse>() {
      @Override
      public CreateStoreTResponse call() throws AlluxioException {
        mKeyValueMaster.createStore(new AlluxioURI(path));
        return new CreateStoreTResponse();
      }
    });
  }

  @Override
  public CompleteStoreTResponse completeStore(final String path, CompleteStoreTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<CompleteStoreTResponse>() {
      @Override
      public CompleteStoreTResponse call() throws AlluxioException {
        mKeyValueMaster.completeStore(new AlluxioURI(path));
        return new CompleteStoreTResponse();
      }
    });
  }

  @Override
  public DeleteStoreTResponse deleteStore(final String path, DeleteStoreTOptions options)
      throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<DeleteStoreTResponse>() {
      @Override
      public DeleteStoreTResponse call() throws AlluxioException, IOException {
        mKeyValueMaster.deleteStore(new AlluxioURI(path));
        return new DeleteStoreTResponse();
      }
    });
  }

  @Override
  public GetPartitionInfoTResponse getPartitionInfo(final String path,
      GetPartitionInfoTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallable<GetPartitionInfoTResponse>() {
      @Override
      public GetPartitionInfoTResponse call() throws AlluxioException {
        return new GetPartitionInfoTResponse(
            mKeyValueMaster.getPartitionInfo(new AlluxioURI(path)));
      }
    });
  }

  @Override
  public MergeStoreTResponse mergeStore(final String fromPath, final String toPath,
      MergeStoreTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<MergeStoreTResponse>() {
      @Override
      public MergeStoreTResponse call() throws AlluxioException, IOException {
        mKeyValueMaster.mergeStore(new AlluxioURI(fromPath), new AlluxioURI(toPath));
        return new MergeStoreTResponse();
      }
    });
  }

  @Override
  public RenameStoreTResponse renameStore(final String oldPath, final String newPath,
      RenameStoreTOptions options) throws AlluxioTException {
    return RpcUtils.call(LOG, new RpcCallableThrowsIOException<RenameStoreTResponse>() {
      @Override
      public RenameStoreTResponse call() throws AlluxioException, IOException {
        mKeyValueMaster.renameStore(new AlluxioURI(oldPath), new AlluxioURI(newPath));
        return new RenameStoreTResponse();
      }
    });
  }
}
