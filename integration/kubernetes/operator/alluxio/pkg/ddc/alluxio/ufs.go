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

	"github.com/Alluxio/alluxio/pkg/common"
	"github.com/Alluxio/alluxio/pkg/ddc/alluxio/operations"
)

// Syn the UFS
func (e *AlluxioEngine) SyncUFS() (changed bool, err error) {
	return false, nil
}

func (e *AlluxioEngine) UsedStorageBytes() (value uint64, err error) {
	return e.usedStorageBytesInternal()
}

func (e *AlluxioEngine) FreeStorageBytes() (value uint64, err error) {
	return e.freeStorageBytesInternal()
}
func (e *AlluxioEngine) TotalStorageBytes() (value uint64, err error) {
	return e.totalStorageBytesInternal()
}
func (e *AlluxioEngine) TotalFileNums() (value uint64, err error) {
	return e.totalFileNumsInternal()
}

func (e *AlluxioEngine) usedStorageBytesInternal() (value uint64, err error) {
	return
}

func (e *AlluxioEngine) freeStorageBytesInternal() (value uint64, err error) {
	return
}

func (e *AlluxioEngine) totalStorageBytesInternal() (total uint64, err error) {
	var (
		podName       = e.Config.Name + "-" + e.Type() + "-master-0"
		containerName = "alluxio-master"
		namespace     = common.ALLUXIO_NAMESPACE
	)

	fileUitls := operations.NewAlluxioFileUtils(podName, containerName, namespace, e.Log)
	_, _, total, err = fileUitls.Count("/")
	if err != nil {
		return
	}

	return
}

func (e *AlluxioEngine) totalFileNumsInternal() (fileCount uint64, err error) {
	var (
		podName       = e.Config.Name + "-" + e.Type() + "-master-0"
		containerName = "alluxio-master"
		namespace     = common.ALLUXIO_NAMESPACE
	)

	fileUitls := operations.NewAlluxioFileUtils(podName, containerName, namespace, e.Log)
	fileCount, _, _, err = fileUitls.Count("/")
	if err != nil {
		return
	}

	return
}

// Prepare the mount and metadata info
func (e *AlluxioEngine) PrepareUFS() (err error) {
	var (
		podName       = e.Config.Name + "-" + e.Type() + "-master-0"
		containerName = "alluxio-master"
		namespace     = common.ALLUXIO_NAMESPACE
	)

	fileUitls := operations.NewAlluxioFileUtils(podName, containerName, namespace, e.Log)

	ready := fileUitls.Ready()
	if !ready {
		return fmt.Errorf("The UFS is not ready")
	}

	//1. make mount
	for _, mount := range e.Config.Mounts {
		alluxioPath := fmt.Sprintf("/%s", mount.Name)
		mounted, err := fileUitls.IsMounted(alluxioPath)
		e.Log.Info("Check if the alluxio path is mounted.", "alluxioPath", alluxioPath, "mounted", mounted)
		if err != nil {
			return err
		}

		if !mounted {
			err = fileUitls.Mount(alluxioPath, mount.MountPoint, mount.Options, mount.ReadOnly, mount.Shared)
			if err != nil {
				return err
			}
		}

	}

	//2. load the metadata
	return fileUitls.LoadMetaData("/")

}

// du the ufs
func (e *AlluxioEngine) du() (ufs uint64, cached uint64, err error) {
	var (
		podName       = e.Config.Name + "-" + e.Type() + "-master-0"
		containerName = "alluxio-master"
		namespace     = common.ALLUXIO_NAMESPACE
	)

	fileUitls := operations.NewAlluxioFileUtils(podName, containerName, namespace, e.Log)
	return fileUitls.Du("/")
}

// The low water mark ratio
func (e *AlluxioEngine) LowWaterMarkRatio() float64 {
	return e.Config.LowWaterMarkRatio
}

// The high water mark ratio
func (e *AlluxioEngine) HighWaterMarkRatio() float64 {
	return e.Config.HighWaterMarkRatio
}
