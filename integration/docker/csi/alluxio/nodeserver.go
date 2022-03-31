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
	"fmt"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mount "k8s.io/mount-utils"
)

type nodeServer struct {
	client kubernetes.Clientset
	*csicommon.DefaultNodeServer
	nodeId string
	mutex  sync.Mutex
}

/*
 * If mount in nodeserver, start fuse process
 * If mount in seperate pod, mount bind global mount point to pod mount point
 */
func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeContext()["mountInPod"] == "false" {
		glog.V(4).Infoln("Will start Fuse process in csi-node-plugin pod.")
		return nodePublishVolumeMountProcess(req)
	}
	glog.V(4).Infoln("Will start Fuse process in alluxio-fuse pod.")
	return nodePublishVolumeMountPod(req)
}

func nodePublishVolumeMountProcess(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()

	notMnt, err := ensureMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	mountOptions := req.GetVolumeCapability().GetMount().GetMountFlags()
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	/*
	   https://docs.alluxio.io/os/user/edge/en/api/POSIX-API.html
	   https://github.com/Alluxio/alluxio/blob/master/integration/fuse/bin/alluxio-fuse
	*/

	alluxioPath := req.GetVolumeContext()["alluxioPath"]
	if alluxioPath == "" {
		alluxioPath = "/"
	}

	args := []string{"mount"}
	if len(mountOptions) > 0 {
		args = append(args, "-o", strings.Join(mountOptions, ","))
	}
	args = append(args, targetPath, alluxioPath)
	command := exec.Command("/opt/alluxio/integration/fuse/bin/alluxio-fuse", args...)

	glog.V(4).Infoln(command)
	stdoutStderr, err := command.CombinedOutput()
	glog.V(4).Infoln(string(stdoutStderr))
	if err != nil {
		if os.IsPermission(err) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func nodePublishVolumeMountPod(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	stagingPath := req.GetStagingTargetPath()

	notMnt, err := ensureMountPoint(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if !notMnt {
		glog.V(4).Infoln("target path is already mounted")
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Wait for alluxio-fuse pod finishing mount to global mount point
	i := 0
	for i < 10 {
		time.Sleep(3 * time.Second)
		command := exec.Command("bash", "-c", fmt.Sprintf("mount | grep %v | grep alluxio-fuse", stagingPath))
		stdout, err := command.CombinedOutput()
		if err != nil {
			glog.V(3).Infoln("Alluxio is not mounted.")
		}
		if len(stdout) > 0 {
			break
		}
		i++
	}
	if i == 10 {
		glog.V(3).Infoln("alluxio-fuse is not mounted to global mount point in 30s.")
		return nil, status.Error(codes.DeadlineExceeded, "alluxio-fuse is not mounted to global mount point in 30s")
	}

	// mount bind global mount point to pod mount point
	args := []string{"--bind", stagingPath, targetPath}
	command := exec.Command("mount", args...)
	glog.V(4).Infoln(command)
	stdoutStderr, err := command.CombinedOutput()
	glog.V(4).Infoln(string(stdoutStderr))
	if err != nil {
		if os.IsPermission(err) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

/*
 * Call umount for both mount in nodeserver and in separate pod.
 */
func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	command := exec.Command("umount", targetPath)
	glog.V(4).Infoln(command)
	stdoutStderr, err := command.CombinedOutput()
	if err != nil {
		glog.V(3).Infoln(err)
	}
	glog.V(4).Infoln(string(stdoutStderr))

	err = mount.CleanupMountPoint(targetPath, mount.New(""), false)
	if err != nil {
		glog.V(3).Infoln(err)
	} else {
		glog.V(4).Infof("Succeed in unmounting %s", targetPath)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

/*
 * If mount in node server, do nothing
 * If mount in separate pod, mount Alluxio to global mount point
 */
func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeContext()["mountInPod"] == "true" {
		ns.mutex.Lock()
		defer ns.mutex.Unlock()

		glog.V(4).Infoln("Creating Alluxio-fuse pod and mounting Alluxio to global mount point.")
		fusePod, err := getAndCompleteFusePodObj(ns.nodeId, req)
		if err != nil {
			return nil, err
		}
		if _, err := ns.client.CoreV1().Pods("default").Create(fusePod); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to launch Fuse Pod at %v.\n%v", ns.nodeId, err.Error())
		}
		glog.V(4).Infoln("Successfully creating Fuse pod.")
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func getAndCompleteFusePodObj(nodeId string, req *csi.NodeStageVolumeRequest) (*v1.Pod, error) {
	csiFuseYaml, err := ioutil.ReadFile("/opt/alluxio/integration/kubernetes/csi/alluxio-csi-fuse.yaml")
	if err != nil {
		glog.V(4).Info("csi-fuse config yaml file not found")
		return nil, status.Errorf(codes.NotFound, "csi-fuse config yaml file not found: %v", err.Error())
	}
	csiFuseObj, grpVerKind, err := scheme.Codecs.UniversalDeserializer().Decode(csiFuseYaml, nil, nil)
	if err != nil {
		glog.V(4).Info("Failed to decode csi-fuse config yaml file")
		return nil, status.Errorf(codes.NotFound, "Failed to decode csi-fuse config yaml file.\n", err.Error())
	}
	// Only support Fuse Pod for now
	if grpVerKind.Kind != "Pod" {
		glog.V(4).Info("csi-fuse only support pod. %v found.")
		return nil, status.Errorf(codes.InvalidArgument, "csi-fuse only support Pod. %v found.\n%v", grpVerKind.Kind, err.Error())
	}
	csiFusePodObj := csiFuseObj.(*v1.Pod)

	// Add nodeName to pod name for uniqueness
	if nodeId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "nodeID is missing in the csi setup.\n%v", err.Error())
	}
	csiFusePodObj.Name = csiFusePodObj.Name + nodeId

	// Set node name for scheduling
	csiFusePodObj.Spec.NodeName = nodeId

	// Set Alluxio path to be mounted
	targetPath := req.GetVolumeContext()["alluxioPath"]
	if targetPath == "" {
		targetPath = "/"
	}
	source := v1.EnvVar{Name: "FUSE_ALLUXIO_PATH", Value: targetPath}
	csiFusePodObj.Spec.Containers[0].Env = append(csiFusePodObj.Spec.Containers[0].Env, source)

	// Set mount path provided by CSI
	mountPoint := v1.EnvVar{Name: "MOUNT_POINT", Value: req.GetStagingTargetPath()}
	csiFusePodObj.Spec.Containers[0].Env = append(csiFusePodObj.Spec.Containers[0].Env, mountPoint)
	// Set pre-stop command (umount) in pod lifecycle
	lifecycle := &v1.Lifecycle {
		PreStop: &v1.Handler {
			Exec: &v1.ExecAction {
				Command: []string{"/opt/alluxio/integration/fuse/bin/alluxio-fuse", "unmount", req.GetStagingTargetPath()},
			},
		},
	}
	csiFusePodObj.Spec.Containers[0].Lifecycle = lifecycle

	// Set fuse mount options
	fuseOptsStr := strings.Join(req.GetVolumeCapability().GetMount().GetMountFlags(), ",")
	csiFusePodObj.Spec.Containers[0].Args = append(csiFusePodObj.Spec.Containers[0].Args, "--fuse-opts=" + fuseOptsStr)

	return csiFusePodObj, nil
}

/*
 * Call umount for both mount in nodeserver and in separate pod
 */
func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	podName := "alluxio-fuse-" + ns.nodeId
	if err := ns.client.CoreV1().Pods("default").Delete(podName, &metav1.DeleteOptions{}); err != nil {
		if strings.Contains(err.Error(), "not found") {
			// Pod not found. Try to clean up the mount point.
			command := exec.Command("umount", req.GetStagingTargetPath())
			glog.V(4).Infoln(command)
			stdoutStderr, err := command.CombinedOutput()
			if err != nil {
				glog.V(3).Infoln(err)
			}
			glog.V(4).Infoln(string(stdoutStderr))
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error deleting fuse pod %v\n%v", podName, err.Error()))
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability {
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}, nil
}

func isCorruptedDir(dir string) bool {
	pathExists, pathErr := mount.PathExists(dir)
	glog.V(3).Infoln("isCorruptedDir(%s) returned with error: (%v, %v)\\n", dir, pathExists, pathErr)
	return pathErr != nil && mount.IsCorruptedMnt(pathErr)
}

func ensureMountPoint(targetPath string) (bool, error) {
	mounter := mount.New(targetPath)
	notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)

	if err == nil {
		return notMnt, nil
	}
	if err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(targetPath, 0750); err != nil {
			return notMnt, err
		}
		return true, nil
	}
	if isCorruptedDir(targetPath) {
		glog.V(3).Infoln("detected corrupted mount for targetPath [%s]", targetPath)
		if err := mounter.Unmount(targetPath); err != nil {
			glog.V(3).Infoln("failed to umount corrupted path [%s]", targetPath)
			return false, err
		}
		return true, nil
	}
	return notMnt, err
}
