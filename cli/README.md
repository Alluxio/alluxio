# Alluxio CLI

The Alluxio command line interface is the single entrypoint for users to:
- Initialize and configure the Alluxio cluster
- Start and stop processes
- Expose information about the running cluster
- Interact with the filesystem, running commands such as `ls` and `cp`
- Perform administrator level actions, such as format or backup

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
