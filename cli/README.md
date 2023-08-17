# Alluxio CLI

The Alluxio command line interface is the single entrypoint for users to:
- Initialize and configure the Alluxio cluster
- Start and stop processes
- Expose information about the running cluster
- Interact with the filesystem, running commands such as `ls` and `cp`
- Perform administrator level actions such as format or backup

The CLI is invoked through the shell script at `bin/cli.sh`.
Commands follow the format of:
```console
bin/cli.sh <service> <operation> [--<flag>[=<value>]] [<args>]
```

Add the `-h` flag to view more details regarding a service or operation.

## Layout and naming conventions

The choice of names for services, operations, and flags should be succinct: short and unambiguous.
Use of a single word is strongly preferred, but otherwise the name parts should be delimited by a dash such as `foo-bar`.

For example, let's assume there is a `mark` operation as part of a `item` service that can be `set` or `unset` on an item `name`.
The recommended format for a command is
```console
bin/cli.sh item mark --set name
bin/cli.sh item mark --unset name
```
where it is expected that either `--set` or `--unset` are specified.
This is preferred over the alternative of two separate commands with `setMark` and `unsetMark` as the operations.

## User input

### Flags and arguments
After selecting the desired command, additional user input can be parsed, as a mix of arguments and/or flags:
- Arguments: `bin/cli.sh command arg1 arg2 ...`
- Flags: `bin/cli.sh command --flag1 --flag2 val2 ...`

Flags are strictly preferred over arguments.
- The flag name conveys context; an argument does not
- Arguments must be ordered; flags can be declared arbitrarily
- Flags can be designated as required to ensure user input.
- Repeated flags can be defined to capture an ordered list of inputs, ex. `--target target1 --target target2`

### Input validation

User inputs should be validated by the CLI command as much as possible as opposed to the resulting invocation.

## Output conventions and java invocation

A majority of commands result in invoking a java class with arguments to execute the expected operation and possibly return some output.
The output returned from the java invocation should tend towards being plain or machine parseable, such as a JSON formatted string,
rather than terminal friendly or human readable format.
When appropriate, the CLI command will default to formatting this output to be terminal friendly, with an option to output in a machine parseable format.

## References

Github CLI guidelines: https://primer.style/design/native/cli/foundations
