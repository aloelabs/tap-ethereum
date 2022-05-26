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
$ npm install -g @superkeyio/block-gobbler
$ block-gobbler COMMAND
running command...
$ block-gobbler (--version)
@superkeyio/block-gobbler/0.0.0 darwin-x64 node-v16.15.0
$ block-gobbler --help [COMMAND]
USAGE
  $ block-gobbler COMMAND
...
```
<!-- usagestop -->
# Commands
<!-- commands -->
* [`block-gobbler blocks`](#block-gobbler-blocks)
* [`block-gobbler events [FILE]`](#block-gobbler-events-file)
* [`block-gobbler getters`](#block-gobbler-getters)
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

## `block-gobbler blocks`

describe the command here

```
USAGE
  $ block-gobbler blocks --rpc <value>

FLAGS
  --rpc=<value>  (required)

DESCRIPTION
  describe the command here
```

_See code: [dist/commands/blocks.ts](https://github.com/superkeyio/tap-ethereum/blob/v0.0.0/dist/commands/blocks.ts)_

## `block-gobbler events [FILE]`

describe the command here

```
USAGE
  $ block-gobbler events [FILE] --rpc <value> --address <value> --event <value> [--startBlock <value>]
    [--endBlock <value>] [--confirmations <value>] [--abiFile <value>] [--abi <value>] [--maxBlockRange <value>]
    [--concurrency <value>]

FLAGS
  --abi=<value>
  --abiFile=<value>
  --address=<value>        (required)
  --concurrency=<value>    [default: 100]
  --confirmations=<value>  [default: 12]
  --endBlock=<value>
  --event=<value>          (required)
  --maxBlockRange=<value>  [default: 10000] Max block range supported by RPC node's eth_getLogs
  --rpc=<value>            (required)
  --startBlock=<value>

DESCRIPTION
  describe the command here
```

_See code: [dist/commands/events.ts](https://github.com/superkeyio/tap-ethereum/blob/v0.0.0/dist/commands/events.ts)_

## `block-gobbler getters`

describe the command here

```
USAGE
  $ block-gobbler getters --rpc <value> --address <value> -g <value> [--startBlock <value>] [--endBlock
    <value>] [--confirmations <value>] [--abiFile <value>] [--abi <value>] [--batchSize <value>] [--concurrency <value>]

FLAGS
  -g, --getter=<value>     (required)
  --abi=<value>
  --abiFile=<value>
  --address=<value>        (required)
  --batchSize=<value>      [default: 10]
  --concurrency=<value>    [default: 100]
  --confirmations=<value>  [default: 12]
  --endBlock=<value>
  --rpc=<value>            (required)
  --startBlock=<value>

DESCRIPTION
  describe the command here
```

_See code: [dist/commands/getters.ts](https://github.com/superkeyio/tap-ethereum/blob/v0.0.0/dist/commands/getters.ts)_

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
