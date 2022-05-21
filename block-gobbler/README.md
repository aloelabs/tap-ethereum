oclif-hello-world
=================

oclif example Hello World CLI

[![oclif](https://img.shields.io/badge/cli-oclif-brightgreen.svg)](https://oclif.io)
[![Version](https://img.shields.io/npm/v/oclif-hello-world.svg)](https://npmjs.org/package/oclif-hello-world)
[![CircleCI](https://circleci.com/gh/oclif/hello-world/tree/main.svg?style=shield)](https://circleci.com/gh/oclif/hello-world/tree/main)
[![Downloads/week](https://img.shields.io/npm/dw/oclif-hello-world.svg)](https://npmjs.org/package/oclif-hello-world)
[![License](https://img.shields.io/npm/l/oclif-hello-world.svg)](https://github.com/oclif/hello-world/blob/main/package.json)

<!-- toc -->
* [Usage](#usage)
* [Commands](#commands)
<!-- tocstop -->
# Usage
<!-- usage -->
```sh-session
$ npm install -g block-gobbler
$ block-gobbler COMMAND
running command...
$ block-gobbler (--version)
block-gobbler/0.0.0 darwin-x64 node-v16.15.0
$ block-gobbler --help [COMMAND]
USAGE
  $ block-gobbler COMMAND
...
```
<!-- usagestop -->
# Commands
<!-- commands -->
* [`block-gobbler hello PERSON`](#block-gobbler-hello-person)
* [`block-gobbler hello world`](#block-gobbler-hello-world)
* [`block-gobbler help [COMMAND]`](#block-gobbler-help-command)
* [`block-gobbler plugins`](#block-gobbler-plugins)
* [`block-gobbler plugins:install PLUGIN...`](#block-gobbler-pluginsinstall-plugin)
* [`block-gobbler plugins:inspect PLUGIN...`](#block-gobbler-pluginsinspect-plugin)
* [`block-gobbler plugins:install PLUGIN...`](#block-gobbler-pluginsinstall-plugin-1)
* [`block-gobbler plugins:link PLUGIN`](#block-gobbler-pluginslink-plugin)
* [`block-gobbler plugins:uninstall PLUGIN...`](#block-gobbler-pluginsuninstall-plugin)
* [`block-gobbler plugins:uninstall PLUGIN...`](#block-gobbler-pluginsuninstall-plugin-1)
* [`block-gobbler plugins:uninstall PLUGIN...`](#block-gobbler-pluginsuninstall-plugin-2)
* [`block-gobbler plugins update`](#block-gobbler-plugins-update)

## `block-gobbler hello PERSON`

Say hello

```
USAGE
  $ block-gobbler hello [PERSON] -f <value>

ARGUMENTS
  PERSON  Person to say hello to

FLAGS
  -f, --from=<value>  (required) Whom is saying hello

DESCRIPTION
  Say hello

EXAMPLES
  $ oex hello friend --from oclif
  hello friend from oclif! (./src/commands/hello/index.ts)
```

_See code: [dist/commands/hello/index.ts](https://github.com/mattevenson/hello-world/blob/v0.0.0/dist/commands/hello/index.ts)_

## `block-gobbler hello world`

Say hello world

```
USAGE
  $ block-gobbler hello world

DESCRIPTION
  Say hello world

EXAMPLES
  $ oex hello world
  hello world! (./src/commands/hello/world.ts)
```

## `block-gobbler help [COMMAND]`

Display help for block-gobbler.

```
USAGE
  $ block-gobbler help [COMMAND] [-n]

ARGUMENTS
  COMMAND  Command to show help for.

FLAGS
  -n, --nested-commands  Include all nested commands in the output.

DESCRIPTION
  Display help for block-gobbler.
```

_See code: [@oclif/plugin-help](https://github.com/oclif/plugin-help/blob/v5.1.10/src/commands/help.ts)_

## `block-gobbler plugins`

List installed plugins.

```
USAGE
  $ block-gobbler plugins [--core]

FLAGS
  --core  Show core plugins.

DESCRIPTION
  List installed plugins.

EXAMPLES
  $ block-gobbler plugins
```

_See code: [@oclif/plugin-plugins](https://github.com/oclif/plugin-plugins/blob/v2.0.11/src/commands/plugins/index.ts)_

## `block-gobbler plugins:install PLUGIN...`

Installs a plugin into the CLI.

```
USAGE
  $ block-gobbler plugins:install PLUGIN...

ARGUMENTS
  PLUGIN  Plugin to install.

FLAGS
  -f, --force    Run yarn install with force flag.
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Installs a plugin into the CLI.

  Can be installed from npm or a git url.

  Installation of a user-installed plugin will override a core plugin.

  e.g. If you have a core plugin that has a 'hello' command, installing a user-installed plugin with a 'hello' command
  will override the core plugin implementation. This is useful if a user needs to update core plugin functionality in
  the CLI without the need to patch and update the whole CLI.

ALIASES
  $ block-gobbler plugins add

EXAMPLES
  $ block-gobbler plugins:install myplugin 

  $ block-gobbler plugins:install https://github.com/someuser/someplugin

  $ block-gobbler plugins:install someuser/someplugin
```

## `block-gobbler plugins:inspect PLUGIN...`

Displays installation properties of a plugin.

```
USAGE
  $ block-gobbler plugins:inspect PLUGIN...

ARGUMENTS
  PLUGIN  [default: .] Plugin to inspect.

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Displays installation properties of a plugin.

EXAMPLES
  $ block-gobbler plugins:inspect myplugin
```

## `block-gobbler plugins:install PLUGIN...`

Installs a plugin into the CLI.

```
USAGE
  $ block-gobbler plugins:install PLUGIN...

ARGUMENTS
  PLUGIN  Plugin to install.

FLAGS
  -f, --force    Run yarn install with force flag.
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Installs a plugin into the CLI.

  Can be installed from npm or a git url.

  Installation of a user-installed plugin will override a core plugin.

  e.g. If you have a core plugin that has a 'hello' command, installing a user-installed plugin with a 'hello' command
  will override the core plugin implementation. This is useful if a user needs to update core plugin functionality in
  the CLI without the need to patch and update the whole CLI.

ALIASES
  $ block-gobbler plugins add

EXAMPLES
  $ block-gobbler plugins:install myplugin 

  $ block-gobbler plugins:install https://github.com/someuser/someplugin

  $ block-gobbler plugins:install someuser/someplugin
```

## `block-gobbler plugins:link PLUGIN`

Links a plugin into the CLI for development.

```
USAGE
  $ block-gobbler plugins:link PLUGIN

ARGUMENTS
  PATH  [default: .] path to plugin

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Links a plugin into the CLI for development.

  Installation of a linked plugin will override a user-installed or core plugin.

  e.g. If you have a user-installed or core plugin that has a 'hello' command, installing a linked plugin with a 'hello'
  command will override the user-installed or core plugin implementation. This is useful for development work.

EXAMPLES
  $ block-gobbler plugins:link myplugin
```

## `block-gobbler plugins:uninstall PLUGIN...`

Removes a plugin from the CLI.

```
USAGE
  $ block-gobbler plugins:uninstall PLUGIN...

ARGUMENTS
  PLUGIN  plugin to uninstall

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Removes a plugin from the CLI.

ALIASES
  $ block-gobbler plugins unlink
  $ block-gobbler plugins remove
```

## `block-gobbler plugins:uninstall PLUGIN...`

Removes a plugin from the CLI.

```
USAGE
  $ block-gobbler plugins:uninstall PLUGIN...

ARGUMENTS
  PLUGIN  plugin to uninstall

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Removes a plugin from the CLI.

ALIASES
  $ block-gobbler plugins unlink
  $ block-gobbler plugins remove
```

## `block-gobbler plugins:uninstall PLUGIN...`

Removes a plugin from the CLI.

```
USAGE
  $ block-gobbler plugins:uninstall PLUGIN...

ARGUMENTS
  PLUGIN  plugin to uninstall

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Removes a plugin from the CLI.

ALIASES
  $ block-gobbler plugins unlink
  $ block-gobbler plugins remove
```

## `block-gobbler plugins update`

Update installed plugins.

```
USAGE
  $ block-gobbler plugins update [-h] [-v]

FLAGS
  -h, --help     Show CLI help.
  -v, --verbose

DESCRIPTION
  Update installed plugins.
```
<!-- commandsstop -->
