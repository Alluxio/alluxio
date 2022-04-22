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
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	mount "k8s.io/mount-utils"
)

type nodeServer struct {
	client kubernetes.Clientset
	*csicommon.DefaultNodeServer
	nodeId string
	mutex  sync.Mutex
}

/*
 * When there is no app pod using the pv, the first app pod using the pv would trigger NodeStageVolume().
 * Only after a successful return, NodePublishVolume() is called.
 * When a pv is already in use and a new app pod uses it as its volume, it would only trigger NodePublishVolume()
 *
 * NodeUnpublishVolume() and NodeUnstageVolume() are the opposites of NodePublishVolume() and NodeStageVolume()
 * When a pv would still be using by other pods after an app pod terminated, only NodeUnpublishVolume() is called.
 * When a pv would not be in use after an app pod terminated, NodeUnpublishVolume() is called. Only after a successful
 * return, NodeUnstageVolume() is called.
 *
 * For more detailed CSI doc, refer to https://github.com/container-storage-interface/spec/blob/master/spec.md
 */

func (ns *nodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeContext()["mountInPod"] == "true" {
		glog.V(4).Infoln("Bind mount staging path (global mount point) to target path (pod volume path).")
		return bindMountGlobalMountPointToPodVolPath(req)
	}
	glog.V(4).Infoln("Mount Alluxio to target path (pod volume path) with AlluxioFuse in CSI node server.")
	return newFuseProcessInNodeServer(req)
}

func newFuseProcessInNodeServer(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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

	extraJavaOptions := req.GetVolumeContext()["javaOptions"]
	alluxioFuseJavaOpts := os.Getenv("ALLUXIO_FUSE_JAVA_OPTS")
	alluxioFuseJavaOpts = "ALLUXIO_FUSE_JAVA_OPTS=" + strings.Join([]string{alluxioFuseJavaOpts,
		extraJavaOptions,
	}, " ")
	command.Env = append(os.Environ(), alluxioFuseJavaOpts)

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

func bindMountGlobalMountPointToPodVolPath(req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
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

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	command := exec.Command("/opt/alluxio/integration/fuse/bin/alluxio-fuse", "umount", targetPath)
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

func (ns *nodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeContext()["mountInPod"] != "true" {
		return &csi.NodeStageVolumeResponse{}, nil
	}
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	glog.V(4).Infoln("Creating Alluxio-fuse pod and mounting Alluxio to global mount point.")
	fusePod, err := getAndCompleteFusePodObj(ns.nodeId, req)
	if err != nil {
		return nil, err
	}
	if _, err := ns.client.CoreV1().Pods(os.Getenv("NAMESPACE")).Create(fusePod); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			glog.V(4).Infof("Fuse pod %s already exists.", fusePod.Name)
			return &csi.NodeStageVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Failed to launch Fuse Pod at %v.\n%v", ns.nodeId, err.Error())
	}
	glog.V(4).Infoln("Successfully creating Fuse pod.")

	// Wait for alluxio-fuse pod finishing mount to global mount point
	retry, err := strconv.Atoi(os.Getenv("FAILURE_THRESHOLD"))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Cannot convert failure threshold %v to int.", os.Getenv("FAILURE_THRESHOLD"))
	}
	timeout, err := strconv.Atoi(os.Getenv("PERIOD_SECONDS"))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Cannot convert period seconds %v to int.", os.Getenv("PERIOD_SECONDS"))
	}
	for i:= 0; i < retry; i++ {
		time.Sleep(time.Duration(timeout) * time.Second)
		command := exec.Command("bash", "-c", fmt.Sprintf("mount | grep %v | grep alluxio-fuse", req.GetStagingTargetPath()))
		stdout, err := command.CombinedOutput()
		if err != nil {
			glog.V(3).Infoln(fmt.Sprintf("Alluxio is not mounted in %v seconds.", i * timeout))
		}
		if len(stdout) > 0 {
			return &csi.NodeStageVolumeResponse{}, nil
		}
	}
	glog.V(3).Infoln(fmt.Sprintf("Time out. Alluxio-fuse is not mounted to global mount point in %vs.", (retry - 1) * timeout))
	return nil, status.Error(codes.DeadlineExceeded, fmt.Sprintf("alluxio-fuse is not mounted to global mount point in %vs", (retry - 1) * timeout))
}

func getAndCompleteFusePodObj(nodeId string, req *csi.NodeStageVolumeRequest) (*v1.Pod, error) {
	csiFusePodObj, err := getFusePodObj()
	if err != nil {
		return nil, err
	}

	// Append volumeId to pod name for uniqueness
	csiFusePodObj.Name = csiFusePodObj.Name + "-" + req.GetVolumeId()

	// Set node name for scheduling
	csiFusePodObj.Spec.NodeName = nodeId

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

	// Set fuse mount point
	csiFusePodObj.Spec.Containers[0].Args = append(csiFusePodObj.Spec.Containers[0].Args, req.GetStagingTargetPath())

	// Set alluxio path to be mounted if set
	alluxioPath := req.GetVolumeContext()["alluxioPath"]
	if alluxioPath != "" {
		csiFusePodObj.Spec.Containers[0].Args = append(csiFusePodObj.Spec.Containers[0].Args, alluxioPath)
	}

	// Update ALLUXIO_FUSE_JAVA_OPTS to include csi client java options
	alluxioCSIFuseJavaOpts :=
		strings.Join([]string{os.Getenv("ALLUXIO_FUSE_JAVA_OPTS"), req.GetVolumeContext()["javaOptions"]}, " ")
	alluxioFuseJavaOptsEnv := v1.EnvVar{Name: "ALLUXIO_FUSE_JAVA_OPTS", Value: alluxioCSIFuseJavaOpts}
	csiFusePodObj.Spec.Containers[0].Env = append(csiFusePodObj.Spec.Containers[0].Env, alluxioFuseJavaOptsEnv)
	return csiFusePodObj, nil
}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	podName := "alluxio-fuse-" + req.GetVolumeId()
	if err := ns.client.CoreV1().Pods(os.Getenv("NAMESPACE")).Delete(podName, &metav1.DeleteOptions{}); err != nil {
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

func getFusePodObj() (*v1.Pod, error) {
	csiFuseYaml, err := ioutil.ReadFile("/opt/alluxio/integration/kubernetes/csi/alluxio-csi-fuse.yaml")
	if err != nil {
		glog.V(3).Info("csi-fuse config yaml file not found")
		return nil, status.Errorf(codes.NotFound, "csi-fuse config yaml file not found: %v", err.Error())
	}
	csiFuseObj, grpVerKind, err := scheme.Codecs.UniversalDeserializer().Decode(csiFuseYaml, nil, nil)
	if err != nil {
		glog.V(3).Info("Failed to decode csi-fuse config yaml file")
		return nil, status.Errorf(codes.Internal, "Failed to decode csi-fuse config yaml file.\n", err.Error())
	}
	// Only support Fuse Pod
	if grpVerKind.Kind != "Pod" {
		glog.V(3).Info("csi-fuse only support pod. %v found.")
		return nil, status.Errorf(codes.InvalidArgument, "csi-fuse only support Pod. %v found.\n%v", grpVerKind.Kind, err.Error())
	}
	return csiFuseObj.(*v1.Pod), nil
}
