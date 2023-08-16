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

package env

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/palantir/stacktrace"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/log"
)

var ProcessRegistry = map[string]Process{}

func RegisterProcess(p Process) Process {
	name := p.Base().Name
	if _, ok := ProcessRegistry[name]; ok {
		panic(fmt.Sprintf("Process %v is already registered", name))
	}
	ProcessRegistry[name] = p
	return p
}

type Process interface {
	Base() *BaseProcess
	SetEnvVars(*viper.Viper)

	Start(*StartProcessCommand) error
	StartCmd(*cobra.Command) *cobra.Command

	Stop(*StopProcessCommand) error
	StopCmd(*cobra.Command) *cobra.Command
}

type BaseProcess struct {
	Name              string
	JavaClassName     string
	JavaOptsEnvVarKey string

	// start
	ProcessOutFile string

	// monitor
	MonitorJavaClassName string
}

func (p *BaseProcess) Launch(start *StartProcessCommand, args []string) error {
	logsDir := Env.EnvVar.GetString(ConfAlluxioLogsDir.EnvVar)
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return stacktrace.Propagate(err, "error creating log directory at %v", logsDir)
	}

	startCmd := exec.Command("nohup", args...)
	for _, k := range Env.EnvVar.AllKeys() {
		startCmd.Env = append(startCmd.Env, fmt.Sprintf("%s=%v", k, Env.EnvVar.Get(k)))
	}

	outFile := filepath.Join(Env.EnvVar.GetString(ConfAlluxioLogsDir.EnvVar), p.ProcessOutFile)
	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return stacktrace.Propagate(err, "error opening file at %v", outFile)
	}
	startCmd.Stdout = f
	startCmd.Stderr = f

	log.Logger.Infof("Starting %v", p.Name)
	log.Logger.Debugf("%v > %v 2>&1 &", startCmd.String(), outFile)
	if err := startCmd.Start(); err != nil {
		return stacktrace.Propagate(err, "error starting %v", p.Name)
	}

	if start.AsyncStart {
		log.Logger.Warnf("Skipping monitor checks for %v", p.Name)
	} else {
		// wait a little for main process to start before starting monitor process
		time.Sleep(500 * time.Millisecond)
		if err := p.Monitor(); err != nil {
			return stacktrace.Propagate(err, "error monitoring process after launching")
		}
	}
	return nil
}

var debugOptsToRemove = []*regexp.Regexp{
	regexp.MustCompile(`-agentlib:jdwp=transport=dt_socket.*address=[0-9]*`),
	regexp.MustCompile(`-Dcom.sun.management.jmxremote.port=[0-9]*`),
}

// Monitor runs the corresponding monitoring java class for the process, running the command
// ${JAVA} -cp ${ALLUXIO_CLIENT_CLASSPATH} ${ALLUXIO_MONITOR_JAVA_OPTS} {monitor java class}
// where ${ALLUXIO_MONITOR_JAVA_OPTS} is equivalent to the process specific JAVA_OPTS
// but with several debugging related options removed
func (p *BaseProcess) Monitor() error {
	cmdArgs := []string{"-cp", Env.EnvVar.GetString(EnvAlluxioClientClasspath)}

	javaOpts := strings.TrimSpace(Env.EnvVar.GetString(p.JavaOptsEnvVarKey))
	// remove debugging options from java opts for monitor process
	for _, re := range debugOptsToRemove {
		javaOpts = re.ReplaceAllString(javaOpts, "")
	}
	cmdArgs = append(cmdArgs, strings.Split(javaOpts, " ")...)

	cmdArgs = append(cmdArgs, p.MonitorJavaClassName)

	cmd := exec.Command(Env.EnvVar.GetString(ConfJava.EnvVar), cmdArgs...)
	for _, k := range Env.EnvVar.AllKeys() {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%v", k, Env.EnvVar.Get(k)))
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Logger.Debugln(cmd.String())
	if err := cmd.Run(); err != nil {
		log.Logger.Errorf("[ FAILED ] The %v process is not serving requests", p.Name)
		return stacktrace.Propagate(err, "error running monitor command for %v", p.Name)
	}
	log.Logger.Infof("[ OK ] The %v process is in a healthy state", p.Name)
	return nil
}

func (p *BaseProcess) Stop(cmd *StopProcessCommand) error {
	processes, err := process.Processes()
	if err != nil {
		return stacktrace.Propagate(err, "error listing processes")
	}
	var matchingProcesses []*process.Process
	for _, ps := range processes {
		cmdline, err := ps.Cmdline()
		if err != nil {
			// process may have been terminated since listing
			continue
		}
		if strings.Contains(cmdline, p.JavaClassName) {
			matchingProcesses = append(matchingProcesses, ps)
		}
	}
	if len(matchingProcesses) == 0 {
		log.Logger.Warnf("No process to stop because could not find running process matching %v", p.JavaClassName)
		return nil
	}
	log.Logger.Infof("Found %v running process(es) matching %v", len(matchingProcesses), p.JavaClassName)
	wg, errs := &sync.WaitGroup{}, make([]error, len(matchingProcesses))
	for i, ps := range matchingProcesses {
		i, ps := i, ps
		wg.Add(1)
		go func() {
			defer wg.Done()
			pid := int(ps.Pid)
			proc, err := os.FindProcess(pid)
			if err != nil {
				errs[i] = stacktrace.Propagate(err, "error finding process with pid %v", pid)
				return
			}
			if err := proc.Signal(syscall.SIGTERM); err != nil { // syscall.SIGTERM = kill -15
				errs[i] = stacktrace.Propagate(err, "error sending TERM signal to process for %v with pid %v", p.JavaClassName, pid)
				return
			}
			// wait for process to exit
			const killTimeoutSec = 120
			if ok := waitForProcessExit(proc, killTimeoutSec); !ok {
				if cmd.SoftKill {
					errs[i] = fmt.Errorf("Process for %v with pid %v did not terminate after %v seconds", p.JavaClassName, pid, killTimeoutSec)
					return
				} else {
					log.Logger.Warnf("Force killing process for %v with pid %v", p.JavaClassName, pid)
					if err := proc.Signal(syscall.SIGKILL); err != nil { // syscall.SIGKILL = kill -9
						errs[i] = stacktrace.Propagate(err, "error sending KILL signal to process for %v with pid %v", p.JavaClassName, pid)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	var errMsg []string
	for _, err := range errs {
		if err != nil {
			errMsg = append(errMsg, fmt.Sprintf("Failed to kill process: %v", err.Error()))
		}
	}
	log.Logger.Infof("Successfully killed %v process(es)", len(matchingProcesses)-len(errMsg))
	if len(errMsg) != 0 {
		log.Logger.Errorf("Failed to kill %v process(es):", len(errMsg))
		for _, msg := range errMsg {
			log.Logger.Errorln(msg)
		}
	}

	return nil
}

func waitForProcessExit(proc *os.Process, killTimeoutSec int) bool {
	timeout := time.After(time.Duration(killTimeoutSec) * time.Second)
	tick := time.Tick(time.Second * 10)
	for {
		select {
		case <-timeout:
			return false
		case <-tick:
			if err := proc.Signal(syscall.Signal(0)); err != nil {
				// process is not found, done
				return true
			}
		}
	}
}
