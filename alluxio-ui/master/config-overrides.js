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
    config.resolve.plugins[1].appSrcs = [srcPath, commonUiSrcPath];
    config.module.rules[1].include = [srcPath, commonUiSrcPath];
    config.module.rules[2].oneOf[1].include = [srcPath, commonUiSrcPath];
    config.plugins[9].options.watch = [srcPath, commonUiSrcPath];
    config.plugins[9].watch = [srcPath, commonUiSrcPath];
    return config;
  }
};
