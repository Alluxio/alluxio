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

var processRegistry = map[string]Process{}

func RegisterProcess(p Process) Process {
	name := p.Base().Name
	if _, ok := processRegistry[name]; ok {
		panic(fmt.Sprintf("Process %v is already registered", name))
	}
	processRegistry[name] = p
	return p
}

func InitProcessCommandTree(rootCmd *cobra.Command) {
	processCmd := &cobra.Command{
		Use:   "process",
		Short: "Manage Alluxio processes",
	}
	rootCmd.AddCommand(processCmd)

	for _, p := range processRegistry {
		p.InitCommandTree(processCmd)
	}
}

type Process interface {
	Base() *BaseProcess
	InitCommandTree(*cobra.Command)
	SetEnvVars(*viper.Viper)
	Start() error
	Stop() error
}

type BaseProcess struct {
	Name              string
	JavaClassName     string
	JavaOptsEnvVarKey string

	// TODO: where is entrypoint?
	DefaultJavaOpts string

	// start
	AsyncStart      bool
	ProcessOutFile  string
	SkipKillOnStart bool

	// stop
	SoftKill bool

	// monitor
	MonitorJavaClassName string
}

func NewProcessStartCmd(p Process) *cobra.Command {
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the process",
		RunE: func(cmd *cobra.Command, args []string) error {
			if !p.Base().SkipKillOnStart {
				_ = p.Stop()
			}
			return p.Start()
		},
	}
	startCmd.Flags().BoolVarP(&p.Base().SkipKillOnStart, "skipKillOnStart", "N", false, "Avoid killing previous running processes when starting")
	startCmd.Flags().BoolVarP(&p.Base().AsyncStart, "startAsync", "a", false, "Asynchronously start processes without monitoring for start completion")
	return startCmd
}

func (p *BaseProcess) Launch(args []string) error {
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

	if p.AsyncStart {
		log.Logger.Warnf("Skipping monitor checks for %v", p.Name)
	} else {
		if err := p.Monitor(); err != nil {
			return stacktrace.Propagate(err, "error monitoring process after launching")
		}
	}

	return nil
}

func NewProcessStopCmd(p Process) *cobra.Command {
	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the process",
		RunE: func(cmd *cobra.Command, args []string) error {
			return p.Stop()
		},
	}
	stopCmd.Flags().BoolVarP(&p.Base().SoftKill, "softKill", "s", false, "Soft kill only, don't forcibly kill the process")
	return stopCmd
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

	if err := cmd.Run(); err != nil {
		log.Logger.Errorf("[ FAILED ] The %v process is not serving requests", p.Name)
		return stacktrace.Propagate(err, "error running monitor command for %v", p.Name)
	}
	log.Logger.Infof("[ OK ] The %v process is in a healthy state", p.Name)
	return nil
}

func (p *BaseProcess) Stop() error {
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
				if p.SoftKill {
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
