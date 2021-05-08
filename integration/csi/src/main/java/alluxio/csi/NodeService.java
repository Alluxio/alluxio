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

package alluxio.csi;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateDirectoryPOptions;

import csi.v1.Csi.NodeGetCapabilitiesRequest;
import csi.v1.Csi.NodeGetCapabilitiesResponse;
import csi.v1.Csi.NodeGetInfoRequest;
import csi.v1.Csi.NodeGetInfoResponse;
import csi.v1.Csi.NodePublishVolumeRequest;
import csi.v1.Csi.NodePublishVolumeResponse;
import csi.v1.Csi.NodeUnpublishVolumeRequest;
import csi.v1.Csi.NodeUnpublishVolumeResponse;
import csi.v1.NodeGrpc.NodeImplBase;

import io.grpc.stub.StreamObserver;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the CSI node service.
 */
public class NodeService extends NodeImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(
      NodeService.class);

  private final String mMountCommand;
  private final String mAlluxioRoot;
  private final FileSystem mFileSystem;

  /**
   * @param fs Alluxio file system
   * @param conf Alluxio configuration
   */
  public NodeService(FileSystem fs, AlluxioConfiguration conf) {
    mMountCommand = conf.get(PropertyKey.CSI_MOUNT_COMMAND);
    mAlluxioRoot = conf.get(PropertyKey.CSI_ALLUXIO_ROOT);
    mFileSystem = fs;
  }

  @Override
  public void nodePublishVolume(NodePublishVolumeRequest request,
      StreamObserver<NodePublishVolumeResponse> responseObserver) {

    try {
      Files.createDirectories(Paths.get(request.getTargetPath()));
      AlluxioURI alluxioVolumePath =
          new AlluxioURI(mAlluxioRoot).join(request.getVolumeId());
      mFileSystem.createDirectory(new AlluxioURI("/" + request.getVolumeId()),
          CreateDirectoryPOptions.newBuilder()
              .setAllowExists(true)
              .build());
      // TODO(maobaolong): mount an ufs as volume directory on the alluxio filesystem dynamically.
      String command =
          String.format(mMountCommand,
              request.getTargetPath(),
              alluxioVolumePath.toString());
      LOG.info("Executing {}", command);

      executeCommand(command);

      responseObserver.onNext(NodePublishVolumeResponse.newBuilder()
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private void executeCommand(String command)
      throws IOException, InterruptedException {
    Process exec = Runtime.getRuntime().exec(command);
    exec.waitFor(10, TimeUnit.SECONDS);

    LOG.info("Command is executed with  stdout: {}, stderr: {}",
        IOUtils.toString(exec.getInputStream(), StandardCharsets.UTF_8),
        IOUtils.toString(exec.getErrorStream(), StandardCharsets.UTF_8));
    if (exec.exitValue() != 0) {
      throw new RuntimeException(String
          .format("Return code of the command %s was %d", command,
              exec.exitValue()));
    }
  }

  @Override
  public void nodeUnpublishVolume(NodeUnpublishVolumeRequest request,
      StreamObserver<NodeUnpublishVolumeResponse> responseObserver) {
    String umountCommand =
        String.format("fusermount -u %s", request.getTargetPath());
    LOG.info("Executing {}", umountCommand);

    // TODO(maobaolong): cleanup the volume directory.
    try {
      executeCommand(umountCommand);

      responseObserver.onNext(NodeUnpublishVolumeResponse.newBuilder()
          .build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  @Override
  public void nodeGetCapabilities(NodeGetCapabilitiesRequest request,
      StreamObserver<NodeGetCapabilitiesResponse> responseObserver) {
    NodeGetCapabilitiesResponse response =
        NodeGetCapabilitiesResponse.newBuilder()
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void nodeGetInfo(NodeGetInfoRequest request,
      StreamObserver<NodeGetInfoResponse> responseObserver) {
    NodeGetInfoResponse response;
    try {
      response = NodeGetInfoResponse.newBuilder()
          .setNodeId(InetAddress.getLocalHost().getHostName())
          .build();
      responseObserver.onNext(response);
      responseObserver.onCompleted();
    } catch (UnknownHostException e) {
      responseObserver.onError(e);
    }
  }
}
