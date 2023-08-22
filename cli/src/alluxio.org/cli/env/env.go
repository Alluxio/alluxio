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
	"sort"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"alluxio.org/log"
)

const (
	userLoggerType = "USER_LOGGER"
)

// Env is a global instance of the Alluxio environment
// It is assumed that InitAlluxioEnv() is run before access
// The package level variable exists to allow access by cobra.Command.RunE functions
var Env *AlluxioEnv

type AlluxioEnv struct {
	RootPath string
	Version  string
	EnvVar   *viper.Viper
}

func InitAlluxioEnv(rootPath string, jarEnvVars map[string]string, appendClasspathJars map[string]func(*viper.Viper, string) string) error {
	/*
		Note the precedence order of viper, where earlier is retrieved first:
		- Set, env, config, default (https://pkg.go.dev/github.com/dvln/viper#section-readme)
		Since env > config, explicitly set any values read from config to take precedence over env
		- Use single instance of viper to set values and retrieve values within go logic
		- Use local viper instances to read configuration files to avoid confusion in precedence order
		This effectively negates the possibility of retrieving config values to simplify the precedence order to be
		- Set, env, default
	*/
	envVar := viper.New()
	envVar.AutomaticEnv() // override defaults with existing environment variable values

	// read VERSION from version.sh
	var ver string
	{
		v := viper.New()
		v.AddConfigPath(filepath.Join(rootPath, "libexec"))
		v.SetConfigName("version.sh")
		v.SetConfigType("env")
		if err := v.ReadInConfig(); err != nil {
			return stacktrace.Propagate(err, "error reading version config")
		}
		ver = v.GetString(envVersion)
		envVar.Set(envVersion, ver)
	}

	// set default values
	for k, v := range map[string]string{
		ConfAlluxioHome.EnvVar:        rootPath,
		ConfAlluxioConfDir.EnvVar:     filepath.Join(rootPath, "conf"),
		ConfAlluxioLogsDir.EnvVar:     filepath.Join(rootPath, "logs"),
		confAlluxioUserLogsDir.EnvVar: filepath.Join(rootPath, "logs", "user"),
	} {
		envVar.SetDefault(k, v)
	}

	// set jar env vars
	for envVarName, jarPathFormat := range jarEnvVars {
		envVar.SetDefault(envVarName, filepath.Join(rootPath, fmt.Sprintf(jarPathFormat, ver)))
	}

	// set user-specified environment variable values from alluxio-env.sh
	{
		// use ALLUXIO_CONF_DIR if set externally, otherwise default to above value
		confPath := envVar.GetString(ConfAlluxioConfDir.EnvVar)
		const alluxioEnv = "alluxio-env.sh"
		log.Logger.Debugf("Loading %v configuration from directory %v", alluxioEnv, confPath)
		v := viper.New()
		if _, err := os.Stat(filepath.Join(confPath, alluxioEnv)); err == nil {
			v.AddConfigPath(confPath)
			v.SetConfigName(alluxioEnv)
			v.SetConfigType("env")
			v.AllowEmptyEnv(true)
			if err := v.ReadInConfig(); err != nil {
				return stacktrace.Propagate(err, "error reading alluxio-env config")
			}
			// for each entry found in alluxio-env.sh, explicitly set to global viper
			for _, k := range v.AllKeys() {
				key, val := strings.ToUpper(k), v.Get(k)
				log.Logger.Debugf("Setting user provided %v=%v", key, val)
				envVar.Set(key, val)
			}
		}
	}

	// set classpath variables which are dependent on user configurable values
	envVar.Set(EnvAlluxioClientClasspath, strings.Join([]string{
		envVar.GetString(ConfAlluxioConfDir.EnvVar) + "/",
		envVar.GetString(confAlluxioClasspath.EnvVar),
		envVar.GetString(EnvAlluxioAssemblyClientJar),
	}, ":"))
	envVar.Set(EnvAlluxioServerClasspath, strings.Join([]string{
		envVar.GetString(ConfAlluxioConfDir.EnvVar) + "/",
		envVar.GetString(confAlluxioClasspath.EnvVar),
		envVar.GetString(EnvAlluxioAssemblyServerJar),
	}, ":"))

	for envVarName, envF := range appendClasspathJars {
		envVar.Set(envVarName, envF(envVar, ver))
	}

	// check java executable and version
	if err := checkAndSetJava(envVar); err != nil {
		return stacktrace.Propagate(err, "error finding installed java")
	}
	if err := checkJavaVersion(envVar.GetString(ConfJava.EnvVar)); err != nil {
		return stacktrace.Propagate(err, "error checking java version compatibility")
	}

	// append default opts to ALLUXIO_JAVA_OPTS
	alluxioJavaOpts := envVar.GetString(ConfAlluxioJavaOpts.EnvVar)
	if alluxioJavaOpts != "" {
		// warn about setting configuration through java opts that should be set through environment variables instead
		for _, c := range []*AlluxioConfigEnvVar{
			ConfAlluxioConfDir,
			ConfAlluxioLogsDir,
			confAlluxioUserLogsDir,
		} {
			if strings.Contains(alluxioJavaOpts, c.configKey) {
				log.Logger.Warnf("Setting %v through %v will be ignored. Use environment variable %v instead.", c.configKey, ConfAlluxioJavaOpts.EnvVar, c.EnvVar)
			}
		}
	}

	for _, c := range []*AlluxioConfigEnvVar{
		ConfAlluxioHome,
		ConfAlluxioConfDir,
		ConfAlluxioLogsDir,
		confAlluxioUserLogsDir,
	} {
		alluxioJavaOpts += c.ToJavaOpt(envVar, true) // mandatory java opts
	}

	for _, c := range []*AlluxioConfigEnvVar{
		confAlluxioRamFolder,
		confAlluxioMasterHostname,
		ConfAlluxioMasterMountTableRootUfs,
		ConfAlluxioMasterJournalType,
		confAlluxioWorkerRamdiskSize,
	} {
		alluxioJavaOpts += c.ToJavaOpt(envVar, false) // optional user provided java opts
	}

	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "log4j.configuration", "file:"+filepath.Join(envVar.GetString(ConfAlluxioConfDir.EnvVar), "log4j.properties"))
	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "org.apache.jasper.compiler.disablejsr199", true)
	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "java.net.preferIPv4Stack", true)
	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "org.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads", false)

	envVar.Set(ConfAlluxioJavaOpts.EnvVar, alluxioJavaOpts)

	for _, p := range ProcessRegistry {
		p.SetEnvVars(envVar)
	}

	// also set user environment variables, as they are not associated with a particular process
	// ALLUXIO_USER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} {user provided opts}
	userJavaOpts := fmt.Sprintf(JavaOptFormat, ConfAlluxioLoggerType, userLoggerType)
	userJavaOpts += envVar.GetString(ConfAlluxioJavaOpts.EnvVar)
	userJavaOpts += envVar.GetString(ConfAlluxioUserJavaOpts.EnvVar)
	envVar.Set(ConfAlluxioUserJavaOpts.EnvVar, strings.TrimSpace(userJavaOpts)) // leading spaces need to be trimmed as a exec.Command argument

	if log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		keys := envVar.AllKeys()
		sort.Strings(keys)
		log.Logger.Debugln("Environment variables:")
		for _, k := range keys {
			log.Logger.Debugf("%v %v", strings.ToUpper(k), envVar.Get(k))
		}
		log.Logger.Debugln()
	}

	Env = &AlluxioEnv{
		RootPath: rootPath,
		Version:  ver,
		EnvVar:   envVar,
	}
	return nil
}

func checkAndSetJava(envVar *viper.Viper) error {
	if envVar.Get(ConfJava.EnvVar) != nil {
		return nil
	}
	// check JAVA_HOME
	if javaHome := envVar.Get(confJavaHome.EnvVar); javaHome != nil {
		javaHomeStr, ok := javaHome.(string)
		if !ok {
			return stacktrace.NewError("error casting %v to string", javaHome)
		}
		javaHomeBinJava := filepath.Join(javaHomeStr, "bin", "java")
		// check if JAVA_HOME exists with valid $JAVA_HOME/bin/java binary
		if _, err := os.Stat(javaHomeBinJava); err == nil {
			envVar.Set(ConfJava.EnvVar, javaHomeBinJava)
			return nil
		}
	}
	// check if java is available via `PATH` using `which`
	whichJavaPath, err := exec.Command("which", "java").Output()
	if err == nil {
		envVar.Set(ConfJava.EnvVar, strings.TrimSpace(string(whichJavaPath)))
		return nil
	}
	// cannot find java
	// - ${JAVA} is not set
	// - ${JAVA_HOME}/bin/java is not a valid path
	// - java is not found as part of ${PATH}
	return stacktrace.NewError(`Error: Cannot find 'java' on path or under $JAVA_HOME/bin/. Please set %v in alluxio-env.sh or user bash profile.`, confJavaHome.EnvVar)
}

var (
	// matches the first 2 numbers in the version string encapsulated by double quotes, ex. "11.0.19" -> 11.0
	javaVersionRe = regexp.MustCompile(`.*"(?P<majorMinorVer>\d+\.\d+)[\w.-]*".*`)
	// must be either java 8 or 11
	requiredVersionRegex = regexp.MustCompile(`1\.8|11\.0`)
)

func checkJavaVersion(javaPath string) error {
	cmd := exec.Command("bash", "-c", fmt.Sprintf("%v -version", javaPath))
	javaVer, err := cmd.CombinedOutput()
	if err != nil {
		return stacktrace.Propagate(err, "error finding java version from `%v -version`", javaPath)
	}
	matches := javaVersionRe.FindStringSubmatch(string(javaVer))
	if len(matches) != 2 {
		return stacktrace.NewError("java version output does not match expected regex pattern %v\n%v", javaVersionRe.String(), string(javaVer))
	}
	if !requiredVersionRegex.MatchString(matches[1]) {
		return stacktrace.NewError("Error: Alluxio requires Java 1.8 or 11.0, currently Java %v found.", matches[1])
	}
	return nil
}
