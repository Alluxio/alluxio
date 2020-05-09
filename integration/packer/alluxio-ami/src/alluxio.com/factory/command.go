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
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

func Exec(app string, args []string) (string, error) {
	log.Println(fmt.Sprintf("%v %v", app, strings.Join(args, " ")))
	cmd := exec.Command(app, args...)
	stdout, err := cmd.Output()

	if err != nil {
		return "", err
	}
	return string(stdout), nil
}

// RetryWithWait retries until the condition is true or the max attempts is hit, sleeping the given duration in between each attempt
func RetryWithWait(duration time.Duration, attempts int, condition func() (bool, error)) error {
	for i := 0; i < attempts; i++ {
		done, err := condition()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		time.Sleep(duration)
	}
	return fmt.Errorf("retries failed after %d attempts", attempts)
}
