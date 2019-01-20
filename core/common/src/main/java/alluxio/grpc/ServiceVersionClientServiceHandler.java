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

package alluxio.grpc;

import alluxio.Constants;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * This class is a gRPC handler that serves Alluxio service versions.
 */
public final class ServiceVersionClientServiceHandler
    extends ServiceVersionClientServiceGrpc.ServiceVersionClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceVersionClientServiceHandler.class);

  /** Set of services that are going to be recognized by this versioning service. */
  private Set<ServiceType> mServices;

  public ServiceVersionClientServiceHandler(Set<ServiceType> services) {
    mServices = services;
  }

  @Override
  public void getServiceVersion(GetServiceVersionPRequest request,
      StreamObserver<GetServiceVersionPResponse> responseObserver) {

    ServiceType serviceType = request.getServiceType();
    if (serviceType != ServiceType.UNKNOWN_SERVICE && !mServices.contains(serviceType)) {
      responseObserver.onError(Status.NOT_FOUND.asException());
      return;
    }

    long serviceVersion;
    switch (serviceType) {
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
      case META_MASTER_CONFIG_SERVICE:
        serviceVersion = Constants.META_MASTER_CONFIG_SERVICE_VERSION;
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
        serviceVersion = Constants.UNKNOWN_SERVICE_VERSION;
        break;
    }
    responseObserver
        .onNext(GetServiceVersionPResponse.newBuilder().setVersion(serviceVersion).build());
    responseObserver.onCompleted();
  }
}
