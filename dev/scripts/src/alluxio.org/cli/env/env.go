package env

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"alluxio.org/command"
	"alluxio.org/log"
)

const (
	requiredJavaVersion = 11

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

	Processes map[string]Process
}

func (env *AlluxioEnv) CommandF(format string, args ...interface{}) *command.BashBuilder {
	ret := command.NewF(format, args...)
	for _, k := range env.EnvVar.AllKeys() {
		ret.Env(k, env.EnvVar.Get(k))
	}
	return ret
}

func InitAlluxioEnv(rootPath string) error {
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
		confAlluxioHome.EnvVar:        rootPath,
		confAlluxioConfDir.EnvVar:     filepath.Join(rootPath, "conf"),
		ConfAlluxioLogsDir.EnvVar:     filepath.Join(rootPath, "logs"),
		confAlluxioUserLogsDir.EnvVar: filepath.Join(rootPath, "logs", "user"),
		// TODO: add a go build flag to switch this to the finalized tarball path when building the tarball, ex. filepath.Join(rootPath, "assembly", fmt.Sprintf("alluxio-assembly-client-%v.jar", ver))
		envAlluxioAssemblyClientJar: filepath.Join(rootPath, "assembly", "client", "target", fmt.Sprintf("alluxio-assembly-client-%v-jar-with-dependencies.jar", ver)),
		envAlluxioAssemblyServerJar: filepath.Join(rootPath, "assembly", "server", "target", fmt.Sprintf("alluxio-assembly-server-%v-jar-with-dependencies.jar", ver)),
	} {
		envVar.SetDefault(k, v)
	}

	// set user-specified environment variable values from alluxio-env.sh
	{
		// use ALLUXIO_CONF_DIR if set externally, otherwise default to above value
		confPath := envVar.GetString(confAlluxioConfDir.EnvVar)
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
		envVar.GetString(confAlluxioConfDir.EnvVar) + "/",
		envVar.GetString(confAlluxioClasspath.EnvVar),
		envVar.GetString(envAlluxioAssemblyClientJar),
		filepath.Join(envVar.GetString(confAlluxioHome.EnvVar), "lib", fmt.Sprintf("alluxio-integration-tools-validation-%v.jar", ver)),
	}, ":"))
	envVar.Set(EnvAlluxioServerClasspath, strings.Join([]string{
		envVar.GetString(confAlluxioConfDir.EnvVar) + "/",
		envVar.GetString(confAlluxioClasspath.EnvVar),
		envVar.GetString(envAlluxioAssemblyServerJar),
	}, ":"))

	// check java executable and version
	if err := checkAndSetJava(envVar); err != nil {
		return stacktrace.Propagate(err, "error finding installed java")
	}
	if err := checkJavaVersion(envVar.GetString(ConfJava.EnvVar), requiredJavaVersion); err != nil {
		return stacktrace.Propagate(err, "error checking java version compatibility")
	}

	// append default opts to ALLUXIO_JAVA_OPTS
	alluxioJavaOpts := envVar.GetString(ConfAlluxioJavaOpts.EnvVar)
	if alluxioJavaOpts != "" {
		// warn about setting configuration through java opts that should be set through environment variables instead
		for _, c := range []*AlluxioConfigEnvVar{
			confAlluxioConfDir,
			ConfAlluxioLogsDir,
			confAlluxioUserLogsDir,
		} {
			if strings.Contains(alluxioJavaOpts, c.configKey) {
				log.Logger.Warnf("Setting %v through %v will be ignored. Use environment variable %v instead.", c.configKey, ConfAlluxioJavaOpts.EnvVar, c.EnvVar)
			}
		}
	}

	for _, c := range []*AlluxioConfigEnvVar{
		confAlluxioHome,
		confAlluxioConfDir,
		ConfAlluxioLogsDir,
		confAlluxioUserLogsDir,
	} {
		alluxioJavaOpts += c.ToJavaOpt(envVar, true) // mandatory java opts
	}

	for _, c := range []*AlluxioConfigEnvVar{
		confAlluxioRamFolder,
		confAlluxioMasterHostname,
		confAlluxioMasterMountTableRootUfs,
		confAlluxioWorkerRamdiskSize,
	} {
		alluxioJavaOpts += c.ToJavaOpt(envVar, false) // optional user provided java opts
	}

	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "log4j.configuration", "file:"+filepath.Join(envVar.GetString(confAlluxioConfDir.EnvVar), "log4j.properties"))
	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "org.apache.jasper.compiler.disablejsr199", "true")
	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "java.net.preferIPv4Stack", "true")
	alluxioJavaOpts += fmt.Sprintf(JavaOptFormat, "org.apache.ratis.thirdparty.io.netty.allocator.useCacheForAllThreads", "false")

	envVar.Set(ConfAlluxioJavaOpts.EnvVar, alluxioJavaOpts)

	for _, p := range processRegistry {
		p.SetEnvVars(envVar)
	}

	// also set user environment variables, as they are not associated with a particular process
	// ALLUXIO_USER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} {user provided opts}
	userJavaOpts := fmt.Sprintf(JavaOptFormat, ConfAlluxioLoggerType, userLoggerType)
	userJavaOpts += envVar.GetString(ConfAlluxioJavaOpts.EnvVar)
	userJavaOpts += envVar.GetString(EnvAlluxioUserJavaOpts)
	envVar.Set(EnvAlluxioUserJavaOpts, strings.TrimSpace(userJavaOpts)) // leading spaces need to be trimmed as a exec.Command argument

	if log.Logger.IsLevelEnabled(logrus.DebugLevel) {
		log.Logger.Debugln("Processes:")
		for k := range processRegistry {
			log.Logger.Debugf("%v", k)
		}
		log.Logger.Debugln()

		keys := envVar.AllKeys()
		sort.Strings(keys)
		log.Logger.Debugln("Environment variables:")
		for _, k := range keys {
			log.Logger.Debugf("%v %v", strings.ToUpper(k), envVar.Get(k))
		}
		log.Logger.Debugln()
	}

	Env = &AlluxioEnv{
		RootPath:  rootPath,
		Version:   ver,
		EnvVar:    envVar,
		Processes: processRegistry,
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
	whichJavaPath, err := command.Output("which java")
	if err == nil {
		envVar.Set(ConfJava.EnvVar, string(whichJavaPath))
	}
	// cannot find java
	// - ${JAVA} is not set
	// - ${JAVA_HOME}/bin/java is not a valid path
	// - java is not found as part of ${PATH}
	return stacktrace.NewError(`Error: Cannot find 'java' on path or under $JAVA_HOME/bin/. Please set %v in alluxio-env.sh or user bash profile.`, confJavaHome.EnvVar)
}

// matching the version string encapsulated by double quotes, ex. "11.0.19" where 11 is majorVer and 0 is minorVer
var javaVersionRe = regexp.MustCompile(`.*"(?P<majorVer>\d+)\.(?P<minorVer>\d+)[\w.-]*".*`)

func checkJavaVersion(javaPath string, requiredJavaVersion int) error {
	javaVer, err := command.NewF("%v -version 2>&1", javaPath).Output()
	if err != nil {
		return stacktrace.Propagate(err, "error java version from `java -version`")
	}
	matches := javaVersionRe.FindStringSubmatch(string(javaVer))
	if len(matches) != 3 {
		return stacktrace.NewError("java version output does not match expected regex pattern %v\n%v", javaVersionRe.String(), string(javaVer))
	}
	majorVer, err := strconv.Atoi(matches[1])
	if err != nil {
		return stacktrace.NewError("could not parse major version as an integer: %v", matches[1])
	}
	if majorVer != requiredJavaVersion {
		// java 8 is displayed as 1.8, so also print minor version in error message
		ver := matches[1]
		if _, err := strconv.Atoi(matches[2]); err == nil {
			ver += "." + matches[2]
		}
		return stacktrace.NewError("Error: Alluxio requires Java %v, currently Java %v found.", requiredJavaVersion, ver)
	}
	return nil
}
