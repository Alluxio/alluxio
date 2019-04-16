// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cmdline implements a data-driven mechanism for writing command-line
// programs with built-in support for help.
//
// Commands are linked together to form a command tree.  Since commands may be
// arbitrarily nested within other commands, it's easy to create wrapper
// programs that invoke existing commands.
//
// The syntax for each command-line program is:
//
//   command [flags] [subcommand [flags]]* [args]
//
// Each sequence of flags is associated with the command that immediately
// precedes it.  Flags registered on flag.CommandLine are considered global
// flags, and are allowed anywhere a command-specific flag is allowed.
//
// Pretty usage documentation is automatically generated, and accessible either
// via the standard -h / -help flags from the Go flag package, or a special help
// command.  The help command is automatically appended to commands that already
// have at least one child, and don't already have a "help" child.  Commands
// that do not have any children will exit with an error if invoked with the
// arguments "help ..."; this behavior is relied on when generating recursive
// help to distinguish between external subcommands with and without children.
//
// Pitfalls
//
// The cmdline package must be in full control of flag parsing.  Typically you
// call cmdline.Main in your main function, and flag parsing is taken care of.
// If a more complicated ordering is required, you can call cmdline.Parse and
// then handle any special initialization.
//
// The problem is that flags registered on the root command must be merged
// together with the global flags for the root command to be parsed.  If
// flag.Parse is called before cmdline.Main or cmdline.Parse, it will fail if
// any root command flags are specified on the command line.
package cmdline

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"

	"v.io/x/lib/envvar"
	_ "v.io/x/lib/metadata" // for the -metadata flag
	"v.io/x/lib/timing"
)

// Command represents a single command in a command-line program.  A program
// with subcommands is represented as a root Command with children representing
// each subcommand.  The command graph must be a tree; each command may either
// have no parent (the root) or exactly one parent, and cycles are not allowed.
type Command struct {
	Name     string // Name of the command.
	Short    string // Short description, shown in help called on parent.
	Long     string // Long description, shown in help called on itself.
	ArgsName string // Name of the args, shown in usage line.
	ArgsLong string // Long description of the args, shown in help.

	// Flags defined for this command.  When a flag F is defined on a command C,
	// we allow F to be specified on the command line immediately after C, or
	// after any descendant of C. This FlagSet is only used to specify the
	// flags and their associated value variables, it is never parsed and hence
	// methods on FlagSet that are generally used after parsing cannot be
	// used on Flags. ParsedFlags should be used instead.
	Flags flag.FlagSet
	// ParsedFlags contains the FlagSet created by the Command
	// implementation and that has had its Parse method called. It
	// should be used instead of the Flags field for handling methods
	// that assume Parse has been called (e.g. Parsed, Visit,
	// NArgs etc).
	ParsedFlags *flag.FlagSet
	// DontPropagateFlags indicates whether to prevent the flags defined on this
	// command and the ancestor commands from being propagated to the descendant
	// commands.
	DontPropagateFlags bool
	// DontInheritFlags indicates whether to stop inheriting the flags from the
	// ancestor commands. The flags for the ancestor commands will not be
	// propagated to the child commands as well.
	DontInheritFlags bool

	// Children of the command.
	Children []*Command

	// LookPath indicates whether to look for external subcommands in the
	// directories specified by the PATH environment variable.  The compiled-in
	// children always take precedence; the check for external children only
	// occurs if none of the compiled-in children match.
	//
	// All global flags and flags set on ancestor commands are passed through to
	// the external child.
	LookPath bool

	// Runner that runs the command.
	// Use RunnerFunc to adapt regular functions into Runners.
	//
	// At least one of Children or Runner must be specified.  If both are
	// specified, ArgsName and ArgsLong must be empty, meaning the Runner doesn't
	// take any args.  Otherwise there's a possible conflict between child names
	// and the runner args, and an error is returned from Parse.
	Runner Runner

	// Topics that provide additional info via the default help command.
	Topics []Topic
}

// Runner is the interface for running commands.  Return ErrExitCode to indicate
// the command should exit with a specific exit code.
type Runner interface {
	Run(env *Env, args []string) error
}

// RunnerFunc is an adapter that turns regular functions into Runners.
type RunnerFunc func(*Env, []string) error

// Run implements the Runner interface method by calling f(env, args).
func (f RunnerFunc) Run(env *Env, args []string) error {
	return f(env, args)
}

// Topic represents a help topic that is accessed via the help command.
type Topic struct {
	Name  string // Name of the topic.
	Short string // Short description, shown in help for the command.
	Long  string // Long description, shown in help for this topic.
}

// Main implements the main function for the command tree rooted at root.
//
// It initializes a new environment from the underlying operating system, parses
// os.Args[1:] against the root command, and runs the resulting runner.  Calls
// os.Exit with an exit code that is 0 for success, or non-zero for errors.
//
// Most main packages should be implemented as follows:
//
//   var root := &cmdline.Command{...}
//
//   func main() {
//     cmdline.Main(root)
//   }
func Main(root *Command) {
	env := EnvFromOS()
	if env.Timer != nil && len(env.Timer.Intervals) > 0 {
		env.Timer.Intervals[0].Name = pathName(env.prefix(), []*Command{root})
	}
	err := ParseAndRun(root, env, os.Args[1:])
	code := ExitCode(err, env.Stderr)
	if *flagTime && env.Timer != nil {
		env.Timer.Finish()
		p := timing.IntervalPrinter{Zero: env.Timer.Zero}
		if err := p.Print(env.Stderr, env.Timer.Intervals, env.Timer.Now()); err != nil {
			code2 := ExitCode(err, env.Stderr)
			if code == 0 {
				code = code2
			}
		}
	}
	os.Exit(code)
}

var flagTime = flag.Bool("time", false, "Dump timing information to stderr before exiting the program.")

// Parse parses args against the command tree rooted at root down to a leaf
// command.  A single path through the command tree is traversed, based on the
// sub-commands specified in args.  Global and command-specific flags are parsed
// as the tree is traversed.
//
// On success returns the runner corresponding to the leaf command, along with
// the args to pass to the runner.  In addition the env.Usage function is set to
// produce a usage message corresponding to the leaf command.
//
// Most main packages should just call Main.  Parse should only be used if
// special processing is required after parsing the args, and before the runner
// is run.  An example:
//
//   var root := &cmdline.Command{...}
//
//   func main() {
//     env := cmdline.EnvFromOS()
//     os.Exit(cmdline.ExitCode(parseAndRun(env), env.Stderr))
//   }
//
//   func parseAndRun(env *cmdline.Env) error {
//     runner, args, err := cmdline.Parse(env, root, os.Args[1:])
//     if err != nil {
//       return err
//     }
//     // ... perform initialization that might parse flags ...
//     return runner.Run(env, args)
//   }
//
// Parse merges root flags into flag.CommandLine and sets ContinueOnError, so
// that subsequent calls to flag.Parsed return true.
func Parse(root *Command, env *Env, args []string) (Runner, []string, error) {
	env.TimerPush("cmdline parse")
	defer env.TimerPop()
	if globalFlags == nil {
		// Initialize our global flags to a cleaned copy.  We don't want the merging
		// in parseFlags to contaminate the global flags, even if Parse is called
		// multiple times, so we keep a single package-level copy.
		cleanFlags(flag.CommandLine)
		globalFlags = copyFlags(flag.CommandLine)
	}
	// Set env.Usage to the usage of the root command, in case the parse fails.
	path := []*Command{root}
	env.Usage = makeHelpRunner(path, env).usageFunc
	cleanTree(root)
	if err := checkTreeInvariants(path, env); err != nil {
		return nil, nil, err
	}
	runner, args, err := root.parse(nil, env, args, make(map[string]string))
	if err != nil {
		return nil, nil, err
	}
	// Clear envvars that start with "CMDLINE_" when returning a user-specified
	// runner, to avoid polluting the environment.  In particular CMDLINE_PREFIX
	// and CMDLINE_FIRST_CALL are only meant to be passed to external children,
	// and shouldn't be propagated through the user's runner.
	switch runner.(type) {
	case helpRunner, binaryRunner:
		// The help and binary runners need the envvars to be set.
	default:
		for key, _ := range env.Vars {
			if strings.HasPrefix(key, "CMDLINE_") {
				delete(env.Vars, key)
				if err := os.Unsetenv(key); err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return runner, args, nil
}

var globalFlags *flag.FlagSet

// ParseAndRun is a convenience that calls Parse, and then calls Run on the
// returned runner with the given env and parsed args.
func ParseAndRun(root *Command, env *Env, args []string) error {
	runner, args, err := Parse(root, env, args)
	if err != nil {
		return err
	}
	env.TimerPush("cmdline run")
	defer env.TimerPop()
	return runner.Run(env, args)
}

func trimSpace(s *string) { *s = strings.TrimSpace(*s) }

func cleanTree(cmd *Command) {
	trimSpace(&cmd.Name)
	trimSpace(&cmd.Short)
	trimSpace(&cmd.Long)
	trimSpace(&cmd.ArgsName)
	trimSpace(&cmd.ArgsLong)
	for tx := range cmd.Topics {
		trimSpace(&cmd.Topics[tx].Name)
		trimSpace(&cmd.Topics[tx].Short)
		trimSpace(&cmd.Topics[tx].Long)
	}
	cleanFlags(&cmd.Flags)
	for _, child := range cmd.Children {
		cleanTree(child)
	}
}

func cleanFlags(flags *flag.FlagSet) {
	flags.VisitAll(func(f *flag.Flag) {
		trimSpace(&f.Usage)
	})
}

func checkTreeInvariants(path []*Command, env *Env) error {
	cmd, cmdPath := path[len(path)-1], pathName(env.prefix(), path)
	// Check that the root name is non-empty.
	if cmdPath == "" {
		return fmt.Errorf(`CODE INVARIANT BROKEN; FIX YOUR CODE

Root command name cannot be empty.`)
	}
	// Check that the children and topic names are non-empty and unique.
	seen := make(map[string]bool)
	checkName := func(name string) error {
		if name == "" {
			return fmt.Errorf(`%v: CODE INVARIANT BROKEN; FIX YOUR CODE

Command and topic names cannot be empty.`, cmdPath)
		}
		if seen[name] {
			return fmt.Errorf(`%v: CODE INVARIANT BROKEN; FIX YOUR CODE

Each command must have unique children and topic names.
Saw %q multiple times.`, cmdPath, name)
		}
		seen[name] = true
		return nil
	}
	for _, child := range cmd.Children {
		if err := checkName(child.Name); err != nil {
			return err
		}
	}
	for _, topic := range cmd.Topics {
		if err := checkName(topic.Name); err != nil {
			return err
		}
	}
	// Check that our Children / Runner invariant is satisfied.  At least one must
	// be specified, and if both are specified then ArgsName and ArgsLong must be
	// empty, meaning the Runner doesn't take any args.
	switch hasC, hasR := len(cmd.Children) > 0, cmd.Runner != nil; {
	case !hasC && !hasR:
		return fmt.Errorf(`%v: CODE INVARIANT BROKEN; FIX YOUR CODE

At least one of Children or Runner must be specified.`, cmdPath)
	case hasC && hasR && (cmd.ArgsName != "" || cmd.ArgsLong != ""):
		return fmt.Errorf(`%v: CODE INVARIANT BROKEN; FIX YOUR CODE

Since both Children and Runner are specified, the Runner cannot take args.
Otherwise a conflict between child names and runner args is possible.`, cmdPath)
	}
	// Check recursively for all children
	for _, child := range cmd.Children {
		if err := checkTreeInvariants(append(path, child), env); err != nil {
			return err
		}
	}
	return nil
}

func pathName(prefix string, path []*Command) string {
	name := prefix
	for _, cmd := range path {
		if name != "" {
			name += " "
		}
		name += cmd.Name
	}
	return name
}

func (cmd *Command) parse(path []*Command, env *Env, args []string, setFlags map[string]string) (Runner, []string, error) {
	path = append(path, cmd)
	cmdPath := pathName(env.prefix(), path)
	runHelp := makeHelpRunner(path, env)
	env.Usage = runHelp.usageFunc
	// Parse flags and retrieve the args remaining after the parse, as well as the
	// flags that were set.
	args, setF, err := parseFlags(path, env, args)
	switch {
	case err == flag.ErrHelp:
		return runHelp, nil, nil
	case err != nil:
		return nil, nil, env.UsageErrorf("%s: %v", cmdPath, err)
	}
	for key, val := range setF {
		setFlags[key] = val
	}
	// First handle the no-args case.
	if len(args) == 0 {
		if cmd.Runner != nil {
			return cmd.Runner, nil, nil
		}
		return nil, nil, env.UsageErrorf("%s: no command specified", cmdPath)
	}
	// INVARIANT: len(args) > 0
	// Look for matching children.
	subName, subArgs := args[0], args[1:]
	if len(cmd.Children) > 0 {
		for _, child := range cmd.Children {
			if child.Name == subName {
				return child.parse(path, env, subArgs, setFlags)
			}
		}
		// Every non-leaf command gets a default help command.
		if helpName == subName {
			return runHelp.newCommand().parse(path, env, subArgs, setFlags)
		}
	}
	if cmd.LookPath {
		// Look for a matching executable in PATH.
		if subCmd, _ := env.LookPath(cmd.Name + "-" + subName); subCmd != "" {
			extArgs := append(flagsAsArgs(setFlags), subArgs...)
			return binaryRunner{subCmd, cmdPath}, extArgs, nil
		}
	}
	// No matching subcommands, check various error cases.
	switch {
	case cmd.Runner == nil:
		return nil, nil, env.UsageErrorf("%s: unknown command %q", cmdPath, subName)
	case cmd.ArgsName == "":
		if len(cmd.Children) > 0 {
			return nil, nil, env.UsageErrorf("%s: unknown command %q", cmdPath, subName)
		}
		return nil, nil, env.UsageErrorf("%s: doesn't take arguments", cmdPath)
	case reflect.DeepEqual(args, []string{helpName, "..."}):
		return nil, nil, env.UsageErrorf("%s: unsupported help invocation", cmdPath)
	}
	// INVARIANT:
	// cmd.Runner != nil && len(args) > 0 &&
	// cmd.ArgsName != "" && args != []string{"help", "..."}
	return cmd.Runner, args, nil
}

// parseFlags parses the flags from args for the command with the given path and
// env.  Returns the remaining non-flag args and the flags that were set.
func parseFlags(path []*Command, env *Env, args []string) ([]string, map[string]string, error) {
	cmd, isRoot := path[len(path)-1], len(path) == 1
	// Parse the merged command-specific and global flags.
	var flags *flag.FlagSet
	if isRoot {
		// The root command is special, due to the pitfall described above in the
		// package doc.  Merge into flag.CommandLine and use that for parsing.  This
		// ensures that subsequent calls to flag.Parsed will return true, so the
		// user can check whether flags have already been parsed.  Global flags take
		// precedence over command flags for the root command.
		flags = flag.CommandLine
		mergeFlags(flags, &cmd.Flags)
	} else {
		// Command flags take precedence over global flags for non-root commands.
		flags = pathFlags(path)
		mergeFlags(flags, globalFlags)
	}
	// Silence the many different ways flags.Parse can produce ugly output; we
	// just want it to return any errors and handle the output ourselves.
	//   1) Set flag.ContinueOnError so that Parse() doesn't exit or panic.
	//   2) Discard all output (can't be nil, that means stderr).
	//   3) Set an empty Usage (can't be nil, that means use the default).
	flags.Init(cmd.Name, flag.ContinueOnError)
	flags.SetOutput(ioutil.Discard)
	flags.Usage = func() {}
	if isRoot {
		// If this is the root command, we must remember to undo the above changes
		// on flag.CommandLine after the parse.  We don't know the original settings
		// of these values, so we just blindly set back to the default values.
		defer func() {
			flags.Init(cmd.Name, flag.ExitOnError)
			flags.SetOutput(nil)
			flags.Usage = func() { env.Usage(env, env.Stderr) }
		}()
	}
	if err := flags.Parse(args); err != nil {
		return nil, nil, err
	}
	cmd.ParsedFlags = flags
	return flags.Args(), extractSetFlags(flags), nil
}

func mergeFlags(dst, src *flag.FlagSet) {
	src.VisitAll(func(f *flag.Flag) {
		// If there is a collision in flag names, the existing flag in dst wins.
		// Note that flag.Var will panic if it sees a collision.
		if dst.Lookup(f.Name) == nil {
			dst.Var(f.Value, f.Name, f.Usage)
			dst.Lookup(f.Name).DefValue = f.DefValue
		}
	})
}

func copyFlags(flags *flag.FlagSet) *flag.FlagSet {
	cp := new(flag.FlagSet)
	mergeFlags(cp, flags)
	return cp
}

// pathFlags returns the flags that are allowed for the last command in the
// path.  Flags defined on ancestors are also allowed, except on "help".
func pathFlags(path []*Command) *flag.FlagSet {
	cmd := path[len(path)-1]
	flags := copyFlags(&cmd.Flags)
	if cmd.Name != helpName && !cmd.DontInheritFlags {
		// Walk backwards to merge flags up to the root command.  If this takes too
		// long, we could consider memoizing previous results.
		for p := len(path) - 2; p >= 0; p-- {
			if path[p].DontPropagateFlags {
				break
			}
			mergeFlags(flags, &path[p].Flags)
			if path[p].DontInheritFlags {
				break
			}
		}
	}
	return flags
}

func extractSetFlags(flags *flag.FlagSet) map[string]string {
	// Use FlagSet.Visit rather than VisitAll to restrict to flags that are set.
	setFlags := make(map[string]string)
	flags.Visit(func(f *flag.Flag) {
		setFlags[f.Name] = f.Value.String()
	})
	return setFlags
}

func flagsAsArgs(x map[string]string) []string {
	var args []string
	for key, val := range x {
		args = append(args, "-"+key+"="+val)
	}
	sort.Strings(args)
	return args
}

// subNames returns the sub names of c which should be ignored when using look
// path to find external binaries.
func (c *Command) subNames(prefix string) map[string]bool {
	m := map[string]bool{prefix + "help": true}
	for _, child := range c.Children {
		m[prefix+child.Name] = true
	}
	return m
}

// ErrExitCode may be returned by Runner.Run to cause the program to exit with a
// specific error code.
type ErrExitCode int

// Error implements the error interface method.
func (x ErrExitCode) Error() string {
	return fmt.Sprintf("exit code %d", x)
}

// ErrUsage indicates an error in command usage; e.g. unknown flags, subcommands
// or args.  It corresponds to exit code 2.
const ErrUsage = ErrExitCode(2)

// ExitCode returns the exit code corresponding to err.
//   0:    if err == nil
//   code: if err is ErrExitCode(code)
//   1:    all other errors
// Writes the error message for "all other errors" to w, if w is non-nil.
func ExitCode(err error, w io.Writer) int {
	if err == nil {
		return 0
	}
	if code, ok := err.(ErrExitCode); ok {
		return int(code)
	}
	if w != nil {
		// We don't print "ERROR: exit code N" above to avoid cluttering the output.
		fmt.Fprintf(w, "ERROR: %v\n", err)
	}
	return 1
}

type binaryRunner struct {
	subCmd  string
	cmdPath string
}

func (b binaryRunner) Run(env *Env, args []string) error {
	env.TimerPush("run " + filepath.Base(b.subCmd))
	defer env.TimerPop()
	vars := envvar.CopyMap(env.Vars)
	vars["CMDLINE_PREFIX"] = b.cmdPath
	cmd := exec.Command(b.subCmd, args...)
	cmd.Stdin = env.Stdin
	cmd.Stdout = env.Stdout
	cmd.Stderr = env.Stderr
	cmd.Env = envvar.MapToSlice(vars)
	err := cmd.Run()
	// Make sure we return the exit code from the binary, if it exited.
	if exitError, ok := err.(*exec.ExitError); ok {
		if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
			return ErrExitCode(status.ExitStatus())
		}
	}
	return err
}
