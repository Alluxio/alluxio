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

package alluxio

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"os/exec"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controllerServer struct {
	*csicommon.DefaultControllerServer
}

func (cs *controllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeID := sanitizeVolumeID(req.GetName())

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("invalid create volume req: %v", req)
		return nil, err
	}

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	capacityBytes := int64(req.GetCapacityRange().GetRequiredBytes())

	params := req.GetParameters()
	pathType, exist := params["pathType"]
	if !exist {
		pathType = "Directory"
	}
	alluxioPath, exist := params["alluxioPath"]
	if !exist {
		return nil, status.Error(codes.InvalidArgument, "alluxioPath missing in parameters")
	}

	switch pathType {
	case "DirectoryOrCreate":
		err := cs.isDirectory(alluxioPath)
		if err == nil {
			glog.V(4).Infof("Directory %s already exists", alluxioPath)
			break
		}
		err = cs.createDirectory(alluxioPath)
		if err != nil {
			return nil, status.Error(codes.Aborted, "Failed to create directory")
		}
	case "Directory":
		err := cs.isDirectory(alluxioPath)
		if err != nil {
			return nil, status.Error(codes.Aborted, "AlluxioPath does not exists or is not a directory")
		}
	}

	glog.V(4).Infof("Creating volume %s", volumeID)
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capacityBytes,
			VolumeContext: params,
		},
	}, nil
}

func (cs *controllerServer) isDirectory(alluxioPath string) error {
	command := exec.Command("/opt/alluxio/bin/alluxio", "fs", "test", "-d", alluxioPath)
	stdoutStderr, err := command.CombinedOutput()
	if err != nil {
		glog.V(2).Infof("Path %s is not a directory, stdout/stderr is: %s, error is %v",
			alluxioPath, string(stdoutStderr), err)
		return err
	}
	glog.V(4).Infof("IsDirectory command stdout/stderr is: %s", string(stdoutStderr))
	return nil
}

func (cs *controllerServer) createDirectory(alluxioPath string) error {
	glog.V(4).Infof("Create directory %s", alluxioPath)
	command := exec.Command("/opt/alluxio/bin/alluxio", "fs", "mkdir", alluxioPath)
	stdoutStderr, err := command.CombinedOutput()
	if err != nil {
		glog.V(2).Infof("Failed to create directory, stdout/stderr is: %s, error is %v", string(stdoutStderr), err)
		return err
	}
	glog.V(4).Infof("CreateDirectory command stdout/stderr is: %s", string(stdoutStderr))
	return nil
}

func (cs *controllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()

	// Check arguments
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}

	if err := cs.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		glog.V(3).Infof("Invalid delete volume req: %v", req)
		return nil, err
	}
	glog.V(4).Infof("Deleting volume %s", volumeID)

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	// Check arguments
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	// We currently only support RWO
	supportedAccessMode := &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	}

	for _, cap := range req.VolumeCapabilities {
		if cap.GetAccessMode().GetMode() != supportedAccessMode.GetMode() {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: "Only single node writer is supported"}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessMode: supportedAccessMode,
				},
			},
		},
	}, nil
}

func (cs *controllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return &csi.ControllerExpandVolumeResponse{}, status.Error(codes.Unimplemented, "ControllerExpandVolume is not implemented")
}

func sanitizeVolumeID(volumeID string) string {
	volumeID = strings.ToLower(volumeID)
	if len(volumeID) > 63 {
		h := sha1.New()
		io.WriteString(h, volumeID)
		volumeID = hex.EncodeToString(h.Sum(nil))
	}
	return volumeID
}
