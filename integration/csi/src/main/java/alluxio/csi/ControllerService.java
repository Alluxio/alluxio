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

import csi.v1.ControllerGrpc.ControllerImplBase;
import csi.v1.Csi.CapacityRange;
import csi.v1.Csi.ControllerGetCapabilitiesRequest;
import csi.v1.Csi.ControllerGetCapabilitiesResponse;
import csi.v1.Csi.ControllerServiceCapability;
import csi.v1.Csi.ControllerServiceCapability.RPC;
import csi.v1.Csi.ControllerServiceCapability.RPC.Type;
import csi.v1.Csi.CreateVolumeRequest;
import csi.v1.Csi.CreateVolumeResponse;
import csi.v1.Csi.DeleteVolumeRequest;
import csi.v1.Csi.DeleteVolumeResponse;
import csi.v1.Csi.Volume;
import io.grpc.stub.StreamObserver;

/**
 * CSI controller service.
 * <p>
 * This service usually runs only once and responsible for the creation of
 * the volume.
 */
public class ControllerService extends ControllerImplBase {

  private long mDefaultVolumeSize;

  /**
   * @param volumeSize default volume size
   */
  public ControllerService(long volumeSize) {
    mDefaultVolumeSize = volumeSize;
  }

  @Override
  public void createVolume(CreateVolumeRequest request,
      StreamObserver<CreateVolumeResponse> responseObserver) {
    long size = findSize(request.getCapacityRange());

    CreateVolumeResponse response = CreateVolumeResponse.newBuilder()
        .setVolume(Volume.newBuilder()
            .setVolumeId(request.getName())
            .setCapacityBytes(size))
        .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private long findSize(CapacityRange capacityRange) {
    if (capacityRange.getRequiredBytes() != 0) {
      return capacityRange.getRequiredBytes();
    } else {
      if (capacityRange.getLimitBytes() != 0) {
        return Math.min(mDefaultVolumeSize, capacityRange.getLimitBytes());
      } else {
        //~1 gig
        return mDefaultVolumeSize;
      }
    }
  }

  @Override
  public void deleteVolume(DeleteVolumeRequest request,
      StreamObserver<DeleteVolumeResponse> responseObserver) {
    DeleteVolumeResponse response = DeleteVolumeResponse.newBuilder()
        .build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void controllerGetCapabilities(
      ControllerGetCapabilitiesRequest request,
      StreamObserver<ControllerGetCapabilitiesResponse> responseObserver) {
    ControllerGetCapabilitiesResponse response =
        ControllerGetCapabilitiesResponse.newBuilder()
            .addCapabilities(
                ControllerServiceCapability.newBuilder().setRpc(
                    RPC.newBuilder().setType(Type.CREATE_DELETE_VOLUME)))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
