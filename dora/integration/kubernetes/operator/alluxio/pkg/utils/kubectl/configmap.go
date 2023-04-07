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

package kubectl

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
)

var log logr.Logger

func init() {
	log = ctrl.Log.WithName("kubectl")
}

/**
*
* create configMap from file
**/
func CreateConfigMapFromFile(name string, key, fileName string, namespace string) (err error) {
	if _, err = os.Stat(fileName); os.IsNotExist(err) {
		return err
	}

	args := []string{"create", "configmap", name,
		"--namespace", namespace,
		fmt.Sprintf("--from-file=%s=%s", key, fileName)}

	out, err := kubectl(args)
	log.V(1).Info("exec: ", "cmd", args)
	log.V(1).Info(fmt.Sprintf("result: %s", string(out)))
	if err != nil {
		log.Error(err, fmt.Sprintf("Failed to execute %v", args))
	}

	return
}

/**
*
* save the key of configMap into a file
**/
func SaveConfigMapToFile(name string, key string, namespace string) (fileName string, err error) {
	binary, err := exec.LookPath(kubectlCmd[0])
	if err != nil {
		return "", err
	}

	file, err := ioutil.TempFile(os.TempDir(), name)
	if err != nil {
		log.Error(err, "failed to create tmp file", "tmpFile", file.Name())
		return fileName, err
	}
	fileName = file.Name()

	args := []string{binary, "get", "configmap", name,
		"--namespace", namespace,
		fmt.Sprintf("-o=jsonpath='{.data.%s}'", key),
		">", fileName}

	log.V(1).Info("exec", "cmd", strings.Join(args, " "))

	cmd := exec.Command("bash", "-c", strings.Join(args, " "))
	// env := os.Environ()
	// if types.KubeConfig != "" {
	// 	env = append(env, fmt.Sprintf("KUBECONFIG=%s", types.KubeConfig))
	// }
	out, err := cmd.Output()
	fmt.Printf("%s", string(out))

	if err != nil {
		return fileName, fmt.Errorf("Failed to execute %s, %v with %v", "kubectl", args, err)
	}
	return fileName, err
}

/**
*
* save the key of configMap into a file
**/
func kubectl(args []string) ([]byte, error) {
	binary, err := exec.LookPath(kubectlCmd[0])
	if err != nil {
		return nil, err
	}

	// 1. prepare the arguments
	// args := []string{"create", "configmap", name, "--namespace", namespace, fmt.Sprintf("--from-file=%s=%s", name, configFileName)}
	log.V(1).Info("exec", "binary", binary, "cmd", strings.Join(args, " "))
	// env := os.Environ()
	// if types.KubeConfig != "" {
	// 	env = append(env, fmt.Sprintf("KUBECONFIG=%s", types.KubeConfig))
	// }

	// return syscall.Exec(cmd, args, env)
	// 2. execute the command
	cmd := exec.Command(binary, args...)
	// cmd.Env = env
	return cmd.CombinedOutput()
}
