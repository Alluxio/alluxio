/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.version;

import alluxio.Constants;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.AlluxioVersionServiceGrpc;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.GetServiceVersionPResponse;

import alluxio.util.RpcUtilsNew;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler that serves Alluxio service versions.
 * TODO(ggezer) Support selective version serving.
 */
public final class AlluxioVersionServiceHandler
    extends AlluxioVersionServiceGrpc.AlluxioVersionServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(alluxio.master.version.AlluxioVersionServiceHandler.class);

  @Override
  public void getServiceVersion(GetServiceVersionPRequest request,
      StreamObserver<GetServiceVersionPResponse> responseObserver) {
    RpcUtilsNew.call(LOG,
        (RpcUtilsNew.RpcCallableThrowsIOException<GetServiceVersionPResponse>) () -> {
          long serviceVersion;
          switch (request.getServiceType()) {
            case FILE_SYSTEM_MASTER_CLIENT_SERVICE:
              serviceVersion = Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION;
              break;
            case FILE_SYSTEM_MASTER_WORKER_SERVICE:
              serviceVersion = Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION;
              break;
            case FILE_SYSTEM_MASTER_JOB_SERVICE:
              serviceVersion = Constants.FILE_SYSTEM_MASTER_JOB_SERVICE_VERSION;
              break;
            case BLOCK_MASTER_CLIENT_SERVICE:
              serviceVersion = Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION;
              break;
            case BLOCK_MASTER_WORKER_SERVICE:
              serviceVersion = Constants.BLOCK_MASTER_WORKER_SERVICE_VERSION;
              break;
            case META_MASTER_CLIENT_SERVICE:
              serviceVersion = Constants.META_MASTER_CLIENT_SERVICE_VERSION;
              break;
            case META_MASTER_MASTER_SERVICE:
              serviceVersion = Constants.META_MASTER_MASTER_SERVICE_VERSION;
              break;
            case METRICS_MASTER_CLIENT_SERVICE:
              serviceVersion = Constants.METRICS_MASTER_CLIENT_SERVICE_VERSION;
              break;
            case JOB_MASTER_CLIENT_SERVICE:
              serviceVersion = Constants.JOB_MASTER_CLIENT_SERVICE_VERSION;
              break;
            case JOB_MASTER_WORKER_SERVICE:
              serviceVersion = Constants.JOB_MASTER_WORKER_SERVICE_VERSION;
              break;
            case KEY_VALUE_MASTER_CLIENT_SERVICE:
              serviceVersion = Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_VERSION;
              break;
            case KEY_VALUE_WORKER_SERVICE:
              serviceVersion = Constants.KEY_VALUE_WORKER_SERVICE_VERSION;
              break;
            default:
              throw new InvalidArgumentException(
                  String.format("Unknown service type: %s", request.getServiceType().name()));
          }
          return GetServiceVersionPResponse.newBuilder().setVersion(serviceVersion).build();
        }, "getServiceVersion", "request", responseObserver, request);
  }
}
