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

package factory

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
)

const (
	alluxioHome             = "/opt/alluxio"
	sitePropsFile           = alluxioHome + "/conf/alluxio-site.properties"
	masterJournalMountPoint = "/mnt/journal"
	workerSSDMountPoint     = "/mnt/ssd"
)

func ConfigureAlluxio(config *UserDataConfigure) error {
	propertiesMap := map[string]string{}
	if err := mountAndConfigureMasterJournal(config, propertiesMap); err != nil {
		return err
	}
	if err := mountAndConfigureWorkerStorage(config, propertiesMap); err != nil {
		return err
	}
	if err := configureMasterInfoProperties(config, propertiesMap); err != nil {
		return err
	}
	configureDailyBackupProperties(config, propertiesMap)
	if err := configureOtherProperties(config, propertiesMap); err != nil {
		return err
	}
	if err := writeAlluxioSiteProperties(propertiesMap); err != nil {
		return err
	}
	return nil
}

func mountAndConfigureMasterJournal(config *UserDataConfigure, propertiesMap map[string]string) error {
	propertiesMap["alluxio.master.journal.type"] = "EMBEDDED"
	if config.AlluxioRole == MasterRole {
		if err := mountVolume(MasterJournalDevice, masterJournalMountPoint); err != nil {
			return err
		}
	}
	propertiesMap["alluxio.master.journal.folder"] = masterJournalMountPoint
	return nil
}

func mountAndConfigureWorkerStorage(config *UserDataConfigure, propertiesMap map[string]string) error {
	if config.AlluxioWorkerMemPercent == "" {
		return errors.New("could not get worker memory percentage information from user data")
	}
	// get instance total memory in MB
	// "vmstat -s -S M" return virtual memory status begins with line "<worker_total_memory> M total memory"
	vmstat, err := Exec("vmstat", []string{ "-s", "-S", "M"})
	if err != nil {
		return err
	}
	index := strings.Index(vmstat, " M total memory")
	if index == -1 {
		return errors.New(fmt.Sprintf("error finding total memory in vmstat %v", vmstat))
	}
	totalMemMb, err := strconv.Atoi(strings.TrimSpace(vmstat[0:index]))
	if err != nil {
		return err
	}
	memPercent, err := strconv.Atoi(config.AlluxioWorkerMemPercent)
	if err != nil {
		return err
	}
	workerMemMb := totalMemMb * memPercent / 100

	ssdSize := config.AlluxioWorkerSSDSize
	if ssdSize == "" || ssdSize == "0" {
		propertiesMap["alluxio.worker.memory.size"] = fmt.Sprintf("%vMB", workerMemMb)
		return nil
	}

	if config.AlluxioRole == WorkerRole {
		if err := mountVolume(WorkerSSDDevice, workerSSDMountPoint); err != nil {
			return err
		}
	}

	propertiesMap["alluxio.worker.tieredstore.levels"] = "2"
	propertiesMap["alluxio.worker.tieredstore.level0.alias"] = "MEM"
	propertiesMap["alluxio.worker.tieredstore.level0.dirs.path"] = "/mnt/ramdisk"
	propertiesMap["alluxio.worker.tieredstore.level0.dirs.quota"] = fmt.Sprintf("%vMB", workerMemMb)
	propertiesMap["alluxio.worker.tieredstore.level1.alias"] = "SSD"
	propertiesMap["alluxio.worker.tieredstore.level1.dirs.path"] = workerSSDMountPoint
	propertiesMap["alluxio.worker.tieredstore.level1.dirs.quota"] = fmt.Sprintf("%vGB", ssdSize)
	return nil
}

func configureMasterInfoProperties(config *UserDataConfigure, propertiesMap map[string]string) error {
	localHostname, err := Exec("curl", []string{"--silent", "--fail", "--show-error", "http://169.254.169.254/2018-09-24/meta-data/hostname"})
	if err != nil {
		return err
	}

	if config.AlluxioRole == MasterRole {
		propertiesMap["alluxio.master.hostname"] = localHostname
	} else if config.AlluxioRole == WorkerRole {
		propertiesMap["alluxio.worker.hostname"] = localHostname
	} else {
		return errors.New(fmt.Sprintf("The given Alluxio role %v is invalid", config.AlluxioRole))
	}

	// alluxio_master_dns has three values:
	// 1) local: this is the master in the single master Alluxio cluster
	// 2) ${master_private_dns}: for workers in the single master Alluxio cluster to set the master hostname
	// 3) "": masters and workers in Alluxio cluster with high availability
	if config.AlluxioMasterDns != nil && config.AlluxioMasterDns != "" {
		if config.AlluxioRole == WorkerRole {
			propertiesMap["alluxio.master.hostname"] = fmt.Sprintf("%v", config.AlluxioMasterDns)
		}
	} else {
		if err := configureMultiMasters(config, propertiesMap); err != nil {
			return err
		}
	}
	return nil
}

func configureDailyBackupProperties(config *UserDataConfigure, propertiesMap map[string]string) {
	if config.AlluxioBackupDirectory != nil && config.AlluxioBackupDirectory != "" {
		propertiesMap["alluxio.master.daily.backup.enabled"] = "true"
		propertiesMap["alluxio.master.backup.directory"] = fmt.Sprintf("%v", config.AlluxioBackupDirectory)
		if config.AlluxioDailyBackupTime != "" {
			propertiesMap["alluxio.master.daily.backup.time"] = config.AlluxioDailyBackupTime
		}
	}
}

func configureOtherProperties(config *UserDataConfigure, propertiesMap map[string]string) error {
	if config.AlluxioUfsAddress == "" {
		return errors.New("Alluxio ufs address must be set")
	}
	propertiesMap["alluxio.master.mount.table.root.ufs"] = config.AlluxioUfsAddress

	hasOtherProperties := config.AlluxioProperties != nil && config.AlluxioProperties != ""
	if hasOtherProperties {
		propertiesString := fmt.Sprintf("%v", config.AlluxioProperties)
		for _, property := range strings.Split(propertiesString, ",") {
			keyValue := strings.Split(strings.TrimSpace(property), "=")
			if len(keyValue) != 2 {
				return errors.New(fmt.Sprintf("error processing alluxio properties %v", config.AlluxioProperties))
			}
			propertiesMap[keyValue[0]] = keyValue[1]
		}
	}

	if !hasOtherProperties || !strings.Contains(fmt.Sprintf("%v", config.AlluxioProperties), "alluxio.security.authorization.permission.enabled") {
		propertiesMap["alluxio.security.authorization.permission.enabled"] = "false"
	}
	return nil
}

func writeAlluxioSiteProperties(propertiesMap map[string]string) error {
	if err := os.RemoveAll(sitePropsFile); err != nil {
		return err
	}
	file, err := os.Create(sitePropsFile)
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range propertiesMap {
		if _, err := file.WriteString(fmt.Sprintf("%v=%v\n", key, value)); err != nil {
			return err
		}
	}
	alluxioUser, err := user.Lookup("alluxio")
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(alluxioUser.Uid)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(alluxioUser.Gid)
	if err != nil {
		return err
	}
	if err := os.Chown(sitePropsFile, uid, gid); err != nil {
		return err
	}
	return nil
}

func mountVolume(device, mountPoint string) error {
	if err := os.MkdirAll(mountPoint, os.ModePerm); err != nil {
		return err
	}
	if _, err := Exec("mkfs", []string{ "-t", "ext4", device}); err != nil {
		return err
	}

	if _, err := Exec("mount", []string{device, mountPoint}); err != nil {
		return err
	}

	fstabFile := "/etc/fstab"
	f, err := os.OpenFile(fstabFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteString(fmt.Sprintf("%v       %v       ext4    defaults,nofail 0       2", device, mountPoint)); err != nil {
		return err
	}

	if _, err := Exec("chown", []string{"-R", "alluxio:alluxio", mountPoint}); err != nil {
		return err
	}

	return nil
}

func configureMultiMasters(config *UserDataConfigure, propertiesMap map[string]string) error {
	region, err := AwsRegion()
	if err != nil {
		return err
	}
	// get the master instance ids of Alluxio masters auto scaling group
	instanceIds, err := Exec("aws", []string{"autoscaling", "describe-auto-scaling-groups", "--auto-scaling-group-names", fmt.Sprintf("%v-AlluxioMasters", config.CftStack), "--region", region, "--query", "AutoScalingGroups[*].Instances[*].InstanceId", "--output", "text"})
	if err != nil {
		return err
	}
	ids := strings.Split(strings.TrimSpace(instanceIds), "\n")

	// get the private DNS name of masters
	args := []string{"ec2", "describe-instances", "--region", region, "--query", "Reservations[*].Instances[*].NetworkInterfaces[*].[PrivateIpAddresses[*].PrivateDnsName]", "--output", "text"}
	args = append(args, "--instance-ids")
	for _, id := range ids {
		args = append(args, id)
	}
	privateDnsNames, err := Exec("aws", args)
	if err != nil {
		return err
	}
	privateDnsNamesArr := strings.Split(strings.TrimSpace(privateDnsNames), "\n")

	embeddedJournalAddresses := strings.Join(privateDnsNamesArr, ":19200,") + ":19200"
	propertiesMap["alluxio.master.embedded.journal.addresses"] = embeddedJournalAddresses
	return nil
}

func AwsRegion() (string, error) {
	region, err := Exec("bash", []string{"-c", "curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone | sed s/[a-z]$//"})
	if err != nil {
		return "", err
	}
	return region, nil
}
