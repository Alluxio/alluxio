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

package processes

import (
	"fmt"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
)

var Proxy = &ProxyProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "proxy",
		JavaClassName:        "alluxio.proxy.AlluxioProxy",
		JavaOpts:             ConfAlluxioProxyJavaOpts,
		ProcessOutFile:       "proxy.out",
		MonitorJavaClassName: "alluxio.proxy.AlluxioProxyMonitor",
	},
}

const (
	confAlluxioProxyAuditLoggerType = "alluxio.proxy.audit.logger.type"
	envAlluxioAuditProxyLogger      = "ALLUXIO_AUDIT_PROXY_LOGGER"
	envAlluxioProxyLogger           = "ALLUXIO_PROXY_LOGGER"
	proxyAuditLoggerType            = "PROXY_AUDIT_LOGGER"
	proxyLoggerType                 = "PROXY_LOGGER"
)

var (
	ConfAlluxioProxyJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_PROXY_JAVA_OPTS",
		IsJavaOpts: true,
	})
	confAlluxioProxyAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_PROXY_ATTACH_OPTS",
		IsJavaOpts: true,
	})
)

type ProxyProcess struct {
	*env.BaseProcess
}

func (p *ProxyProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *ProxyProcess) SetEnvVars(envVar *viper.Viper) {
	envVar.SetDefault(envAlluxioProxyLogger, proxyLoggerType)
	envVar.SetDefault(envAlluxioAuditProxyLogger, proxyAuditLoggerType)
	// ALLUXIO_PROXY_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_PROXY_JAVA_OPTS}
	javaOpts := []string{
		fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioProxyLogger)),
		fmt.Sprintf(env.JavaOptFormat, confAlluxioProxyAuditLoggerType, envVar.Get(envAlluxioAuditProxyLogger)),
	}
	javaOpts = append(javaOpts, env.ConfAlluxioJavaOpts.JavaOptsToArgs(envVar)...)
	javaOpts = append(javaOpts, p.JavaOpts.JavaOptsToArgs(envVar)...)
	envVar.Set(p.JavaOpts.EnvVar, strings.Join(javaOpts, " "))
}

func (p *ProxyProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *ProxyProcess) Start(cmd *env.StartProcessCommand) error {
	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	cmdArgs = append(cmdArgs, confAlluxioProxyAttachOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))
	cmdArgs = append(cmdArgs, p.JavaOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmd, cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}

func (p *ProxyProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}
