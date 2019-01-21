# Alluxio UI

## Building Manually

Maven builds this project automatically, however, if a manual rebuild of the UI, you may do so in the ways below.

### Using Maven (no UI tools installed)

Execute `mvn clean install` from the alluxio-ui directory

### Using Lerna / NPM

#### Prerequisites

We use [NVM](https://github.com/creationix/nvm) to have multiple versions on node on one machine, and [Lerna](https://github.com/lerna/lerna) to handle our UI package dependencies.

1. Install NVM: `brew install nvm`
1. From the `alluxio-ui` directory:
    1. Install Node using NVM. `nvm install`
    1. Install Lerna globally: `npm install -g lerna`
    1. Install UI module dependencies: `lerna bootstrap`

NOTE: Please ensure you are running the correct version of Node before building packages. The expected version can be found both in the [package.json](package.json) file and in the [.nvmrc](.nvmrc) file.

#### Build UI packages

This can be done in two ways:

1. Let lerna build everything: `lerna run build`
1. Build everything independently:
    1. common-ui: `cd alluxio-ui/common-ui && npm run build`
    1. master-ui: `cd alluxio-ui/master-ui && npm run build`
    1. worker-ui: `cd alluxio-ui/worker-ui && npm run build`

#### Test UI packages

This can be done in two ways:

1. Let lerna test everything: `lerna run test-ci`
1. Test everything independently:
    1. common-ui: `cd alluxio-ui/common-ui && npm run test`
    1. master-ui: `cd alluxio-ui/master-ui && npm run test`
    1. worker-ui: `cd alluxio-ui/worker-ui && npm run test`

NOTE: `test-ci` is meant to run tests once and quit (for continuous integration); `test` is meant to run tests for one package and will continuously watch for changes (for active development).

#### Generate UI packages' Test coverage

This can be done in two ways:

1. Let lerna generate coverage for everything: `lerna run coverage-ci`
1. Generate coverage for everything independently:
    1. common-ui: `cd alluxio-ui/common-ui && npm run coverage`
    1. master-ui: `cd alluxio-ui/master-ui && npm run coverage`
    1. worker-ui: `cd alluxio-ui/worker-ui && npm run coverage`

NOTE: `coverage-ci` is meant to run tests once and quit (for continuous integration); "coverage" is meant to run tests for one package and will continuously watch for changes (for active development).

#### Developing within the alluxio-ui project

1. Enable CORS for the alluxio RESTful api endpoints by setting `alluxio.webui.enable.cors=true` in `conf/alluxio-site.properties`
1. Start a development server in a different terminal for each package you intend to work on:
    1. common-ui: `cd alluxio-ui/common-ui && npm run start`
    1. master-ui: `cd alluxio-ui/master-ui && npm run start`
    1. worker-ui: `cd alluxio-ui/worker-ui && npm run start`

    This will open two browser windows with the common-ui at `http://localhost:3000`, the master-ui at `http://localhost:3001`, and the worker-ui at `http://localhost:3002`. Your work will be recompiled and updated as you make changes to each package. You will see compile-time errors in the terminal windows and runtime errors in the browser console.

1. (Optionally) Start a test watcher in a different terminal for each package you intend to work on:
    1. common-ui: `cd alluxio-ui/common-ui && npm run test`
    1. master-ui: `cd alluxio-ui/master-ui && npm run test`
    1. worker-ui: `cd alluxio-ui/worker-ui && npm run test`

    This will continuously run your tests and will show you when tests pass or fail as you work.

1. (Optionally) Execute a test coverage report to investigate places you could test further:
    1. common-ui: `cd alluxio-ui/common-ui && npm run coverage`
    1. master-ui: `cd alluxio-ui/master-ui && npm run coverage`
    1. worker-ui: `cd alluxio-ui/worker-ui && npm run coverage`

    This will also generate a coverage report within each package: `common/coverage/lcov-report/index.html`, `master/coverage/lcov-report/index.html`, `worker/coverage/lcov-report/index.html`. You may also run `coverage-ci` instead of `coverage` in this step if you would like this to execute only once.

