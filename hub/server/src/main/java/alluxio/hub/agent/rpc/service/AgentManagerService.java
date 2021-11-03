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

package alluxio.hub.agent.rpc.service;

import alluxio.RpcUtils;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.hub.agent.process.AgentProcessContext;
import alluxio.hub.proto.AgentDetectPrestoRequest;
import alluxio.hub.proto.AgentDetectPrestoResponse;
import alluxio.hub.proto.AgentFileUploadRequest;
import alluxio.hub.proto.AgentFileUploadResponse;
import alluxio.hub.proto.AgentGetConfigurationSetRequest;
import alluxio.hub.proto.AgentGetConfigurationSetResponse;
import alluxio.hub.proto.AgentListCatalogResponse;
import alluxio.hub.proto.AgentListFileInfo;
import alluxio.hub.proto.AgentListFileRequest;
import alluxio.hub.proto.AgentListFileResponse;
import alluxio.hub.proto.AgentManagerServiceGrpc;
import alluxio.hub.proto.AgentProcessStatusChangeRequest;
import alluxio.hub.proto.AgentProcessStatusChangeResponse;
import alluxio.hub.proto.AgentRemoveFileRequest;
import alluxio.hub.proto.AgentRemoveFileResponse;
import alluxio.hub.proto.AgentSetPrestoConfRequest;
import alluxio.hub.proto.AgentSetPrestoConfResponse;
import alluxio.hub.proto.AgentShutdownRequest;
import alluxio.hub.proto.AgentShutdownResponse;
import alluxio.hub.proto.AgentValidatePrestoConfRequest;
import alluxio.hub.proto.AgentValidatePrestoConfResponse;
import alluxio.hub.proto.AgentWriteConfigurationSetRequest;
import alluxio.hub.proto.AgentWriteConfigurationSetResponse;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.PrestoCatalogListing;
import alluxio.hub.proto.ValidationResult;
import alluxio.hub.proto.ValidationStatus;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The implementation of the RPC service that should be utilized by the hub manager to issue
 * commands to specific nodes.
 */
public class AgentManagerService extends AgentManagerServiceGrpc.AgentManagerServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(AgentManagerService.class);

  public AgentProcessContext mCtx;

  /**
   * The implementation for the manager agent service.
   *
   * @param ctx the agent process context used to execute RPCs
   */
  public AgentManagerService(AgentProcessContext ctx) {
    mCtx = ctx;
  }

  @Override
  public void processStatusChange(AgentProcessStatusChangeRequest request,
      StreamObserver<AgentProcessStatusChangeResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Preconditions.checkArgument(request.hasAction());
      Set<AlluxioNodeType> types = request.hasNodeType()
          ? Collections.singleton(request.getNodeType()) : mCtx.alluxioProcesses();
      for (AlluxioNodeType t : types) {
        try {
          mCtx.changeState(t, request.getAction());
        } catch (IOException | InterruptedException | TimeoutException e) {
          throw AlluxioStatusException.fromCheckedException(e);
        }
      }
      return AgentProcessStatusChangeResponse.newBuilder().setRequest(request).setSuccess(true)
              .build();
    }, "processStatusChange",
       "attempts to change the state of an Alluxio process", responseObserver, request);
  }

  @Override
  public void getConfigurationSet(AgentGetConfigurationSetRequest request,
      StreamObserver<AgentGetConfigurationSetResponse> responseObserver) {
    RpcUtils.call(LOG,
        () -> AgentGetConfigurationSetResponse.newBuilder()
            .setConfSet(mCtx.getConf())
            .build(),
        "getConfigurationSet", "reads all of the alluxio configuration files"
        + " on this node.", responseObserver);
  }

  @Override
  public void writeConfigurationSet(AgentWriteConfigurationSetRequest request,
      StreamObserver<AgentWriteConfigurationSetResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      mCtx.writeConf(request.getConfSet());
      return AgentWriteConfigurationSetResponse
          .newBuilder().build();
    }, "writeConfigurationSet", "writes out the given configuration files.",
            responseObserver);
  }

  @Override
  public void uploadFile(AgentFileUploadRequest request,
      StreamObserver<AgentFileUploadResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      boolean success = mCtx.uploadFile(
          request.getFilePath(), request.getPermission(), request.getFileContent(),
          request.getProcessType());
      return AgentFileUploadResponse
          .newBuilder().setSuccess(success).build();
    }, "uploadFile", "Upload file to the host", responseObserver);
  }

  @Override
  public void removeFile(AgentRemoveFileRequest request,
      StreamObserver<AgentRemoveFileResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      boolean success = mCtx.removeFile(request.getFileName(), request.getProcessType());
      return AgentRemoveFileResponse.newBuilder().setSuccess(success).build();
    }, "removeFile", "Remove a file from the host", responseObserver);
  }

  @Override
  public void listFile(AgentListFileRequest request,
      StreamObserver<AgentListFileResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      List<AgentListFileInfo> fileinfo = mCtx.listFile();
      return AgentListFileResponse
          .newBuilder().addAllFileInfo(fileinfo).build();
    }, "listFile", "List files on the node", responseObserver);
  }

  @Override
  public void detectPresto(AgentDetectPrestoRequest request,
      StreamObserver<AgentDetectPrestoResponse> responseObserver) {
    RpcUtils.call(LOG, () -> AgentDetectPrestoResponse.newBuilder()
        .setDetected(Files.exists(Paths.get(request.getConfDir())))
        .build(), "detectPresto",
        "detects if there is a presto configuration directory at the request's path.",
        responseObserver);
  }

  @Override
  public void listCatalogs(alluxio.hub.proto.AgentListCatalogRequest request,
      io.grpc.stub.StreamObserver<alluxio.hub.proto.AgentListCatalogResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      AgentListCatalogResponse.Builder res = AgentListCatalogResponse.newBuilder();
      Path searchPath = Paths.get(request.getConfDir(), "catalog");
      if (!Files.isDirectory(searchPath)) {
        return res.build();
      }

      Files.list(searchPath)
          .filter(Files::isReadable)
          .filter(Files::isRegularFile)
          .filter(p -> Optional.ofNullable(p.getFileName())
              .map(Objects::toString).orElse("").endsWith(".properties"))
      .map(p -> {
        String filename = Optional.ofNullable(p.getFileName())
            .map(Objects::toString).orElse("");
        String name = Optional.ofNullable(p.getFileName())
            .map(Objects::toString).orElse("").substring(0, filename.indexOf(".properties"));
        try {
          Properties catalogProps = new Properties();
          try (Reader reader = Files.newBufferedReader(p)) {
            catalogProps.load(reader);
          }
          String metastoreUri = catalogProps.getProperty("hive.metastore.uri");
          if (metastoreUri == null) {
            return null; // not a hive metastore.
          }
          return PrestoCatalogListing.newBuilder()
              .setCatalogName(name)
              .setHiveMetastoreUri(metastoreUri)
              .build();
        } catch (IOException e) {
          LOG.warn("Failed to presto catalog file read file {}", p);
          return null;
        }
      })
          .filter(Objects::nonNull)
          .forEach(res::addCatalog);
      return res.build();
    }, "listCatalogs",
        "current catalogs in the presto configuration directory",
        responseObserver);
  }

  @Override
  public void setPrestoConfDir(AgentSetPrestoConfRequest request,
      StreamObserver<AgentSetPrestoConfResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      Properties props = new Properties();
      props.putAll(request.getPropsMap());
      return AgentSetPrestoConfResponse.newBuilder().setSuccess(mCtx.updateConf(props)).build();
    }, "updateConf", "Update alluxio configuration on the cluster",
            responseObserver);
  }

  @Override
  public void validatePrestoConfDir(AgentValidatePrestoConfRequest request,
      StreamObserver<AgentValidatePrestoConfResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      ValidationResult.Builder r = ValidationResult.newBuilder();
      r.setDescription("Checks pre-requisites for the presto configuration directory.");
      r.setName("PrestoConfDirValidation");
      Path prestoConfDir = Paths.get(request.getPrestoConfDir());
      if (!prestoConfDir.isAbsolute()) {
        r.setTestResult(ValidationStatus.FAILED);
        r.setOutput(String.format("Presto conf directory path %s not absolute.", prestoConfDir));
        r.setAdvice(String.format("The directory supplied (%s) should be an absolute path. Please"
                + " reconfigure the presto configuration directory.", prestoConfDir));
      } else if (!Files.exists(prestoConfDir)) {
        r.setTestResult(ValidationStatus.FAILED);
        r.setOutput("Presto conf directory does not exist");
        r.setAdvice(String.format("We couldn't find the specified presto configuration directory "
            + "at %s. Make sure that the presto configuration directory was typed correctly.",
            prestoConfDir));
      } else if (!Files.isDirectory(prestoConfDir)) {
        r.setTestResult(ValidationStatus.FAILED);
        r.setOutput(String.format("Path to presto configuration directory (%s) is not a "
            + "directory", prestoConfDir));
        r.setAdvice(String.format("We detected the presto configuration path (%s) was not a "
                + "directory. This path must point to a directory. Please ensure you have typed "
                + "the path correctly.", prestoConfDir));
      } else {
        r.setTestResult(ValidationStatus.OK);
        r.setOutput("Presto configuration directory passed checks.");
      }
      return AgentValidatePrestoConfResponse.newBuilder().setResult(r.build()).build();
    }, "validatePrestoConfDir", "validate presto configuration parameters",
            responseObserver);
  }

  @Override
  public void shutdown(AgentShutdownRequest request,
      StreamObserver<AgentShutdownResponse> responseObserver) {
    RpcUtils.call(LOG, () -> {
      LOG.warn("Hub Agent is shutting down in 5 seconds!");
      CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS).execute(() -> {
        LOG.info(request.getLogMessage());
        System.exit(request.getExitCode());
      });
      return AgentShutdownResponse.newBuilder().build();
    }, "shutdown", "shuts down the Hub Agent process", responseObserver);
  }
}
