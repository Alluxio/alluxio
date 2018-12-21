const path = require('path');
const fs = require('fs');

const appDirectory = fs.realpathSync(process.cwd());
const resolveApp = relativePath => path.resolve(appDirectory, relativePath);

module.exports = {
  // The Jest config to use when running your jest tests - note that the normal rewires do not
  // work here.
  jest: function (config) {
    // ...add your jest config customisation...
    const commonUiModulesPath = resolveApp('../common/node_modules');
    config.snapshotSerializers = ['enzyme-to-json/serializer'];
    config.moduleDirectories = ['node_modules', commonUiModulesPath];
    return config;
  },
  // The Webpack config to use when compiling your react app for development or production.
  webpack: function (config, env) {
    // ...add your webpack config customisation, rewires, etc...
    const srcPath = resolveApp('src');
    const commonUiSrcPath = resolveApp('../common/src');
    config.module.rules[0].include = [srcPath, commonUiSrcPath];
    config.module.rules[1].oneOf[1].include = [srcPath, commonUiSrcPath];
    config.module.rules[1].oneOf[2].include = [srcPath, commonUiSrcPath];
    return config;
  }
};
