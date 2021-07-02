# Alluxio UI

## Building Manually

Maven builds this project automatically, however, if a manual rebuild of the UI, you may do so in the ways below.

### Using Maven (no UI tools installed)

Execute `mvn clean install` from the webui directory

### Using Lerna / NPM

#### Prerequisites

We use [NVM](https://github.com/creationix/nvm) to have multiple versions on node on one machine, and [Lerna](https://github.com/lerna/lerna) to handle our UI package dependencies.

1. Install NVM: `brew install nvm`
1. From the `webui` directory:
    1. Install Node using NVM. `nvm install`
    1. Install UI module dependencies: `npm run bootstrap`

NOTE: Please ensure you are running the correct version of Node before building packages. The expected version can be found both in the [package.json](package.json) file and in the [.nvmrc](.nvmrc) file.

#### Build UI packages

This can be done in two ways:

1. Let lerna build everything: `npm run build`
1. Build everything independently:
    1. common-ui: `cd webui/common-ui && npm run build`
    1. master-ui: `cd webui/master-ui && npm run build`
    1. worker-ui: `cd webui/worker-ui && npm run build`

#### Test UI packages

This can be done in two ways:

1. Let lerna test everything: `npm run test-ci`
1. Test everything independently:
    1. common-ui: `cd webui/common-ui && npm run test`
    1. master-ui: `cd webui/master-ui && npm run test`
    1. worker-ui: `cd webui/worker-ui && npm run test`

NOTE: `test-ci` is meant to run tests once and quit (for continuous integration); `test` is meant to run tests for one package and will continuously watch for changes (for active development).

#### Generate UI packages' Test coverage

This can be done in two ways:

1. Let lerna generate coverage for everything: `npm run coverage-ci`
1. Generate coverage for everything independently:
    1. common-ui: `cd webui/common-ui && npm run coverage`
    1. master-ui: `cd webui/master-ui && npm run coverage`
    1. worker-ui: `cd webui/worker-ui && npm run coverage`

NOTE: `coverage-ci` is meant to run tests once and quit (for continuous integration); "coverage" is meant to run tests for one package and will continuously watch for changes (for active development).

#### Developing within the webui project

1. Follow the prerequisite instructions above.
1. Enable CORS for the alluxio RESTful api endpoints by setting `alluxio.web.cors.enabled=true` in `conf/alluxio-site.properties`
1. Start a development server in one of the following ways:
    1. For all packages: `npm run start`
    1. For each package independently:
        1. common-ui: `cd webui/common-ui && npm run start`
        1. master-ui: `cd webui/master-ui && npm run start`
        1. worker-ui: `cd webui/worker-ui && npm run start`

    This will open two browser windows with the common-ui at `http://localhost:3000`, the master-ui at `http://localhost:3001`, and the worker-ui at `http://localhost:3002`. Your work will be recompiled and updated as you make changes to each package. You will see compile-time errors in the terminal windows and runtime errors in the browser console.

1. (Optionally) Start a test watcher for each package independently (using lerna for this is possible, but you will not be able to interact with the console):
        1. common-ui: `cd webui/common-ui && npm run test`
        1. master-ui: `cd webui/master-ui && npm run test`
        1. worker-ui: `cd webui/worker-ui && npm run test`

    This will continuously run your tests and will show you when tests pass or fail as you work.

1. (Optionally) Run a test coverage report in one of the following ways:
    1. For all packages: `npm run coverage-ci`
    1. For each package independently:
        1. common-ui: `cd webui/common-ui && npm run coverage`
        1. master-ui: `cd webui/master-ui && npm run coverage`
        1. worker-ui: `cd webui/worker-ui && npm run coverage`

    This will also generate a coverage report within each package: `common/coverage/lcov-report/index.html`, `master/coverage/lcov-report/index.html`, `worker/coverage/lcov-report/index.html`. You may also run `coverage-ci` instead of `coverage` in this step if you would like this to execute only once.

#### Formatting and linting .tsx files

This will format .tsx files based on `.prettierrc.js` and return errors based on rules defined in `.eslintrc.js`.

This can be done in two ways:

1. Let lerna format and lint everything: `npm run format`
1. Build everything independently:
    1. common-ui: `cd webui/common-ui && npm run format`
    1. master-ui: `cd webui/master-ui && npm run format`
    1. worker-ui: `cd webui/worker-ui && npm run format`

#### Shrinkwrapping dependencies

It is sometimes necessary to bump package dependency versions for various reasons:

- Fix vulnerabilities found after an audit.
- Leverage functionality added to a dependency at a later version than the current import.
- Restructure architecture after a paradigm shift.
- Other reasons.

NOTE: Any dependencies that are shared across packages should use identical versions, otherwise lerna will complain about a dependency hoisting error. Please keep shared imported dependency versions synchronized.

Once a dependency changes, it is important to verify any functionality that could be affected by the change. This can be done via unit testing, system testing, and/or old fashioned QA testing.

In order to keep the UI consistent for production builds we shrinkwrap our dependecies using npm-shrinkwrap so that our package versions stay consistent across builds. Shrinkwrapping locks dependency versions so that updates to underlying UI libraries don't affect our build.

Once changes are tested and things work as planned, please run `npm shrinkwrap && lerna exec npm shrinkwrap` to lock package dependency versions.
