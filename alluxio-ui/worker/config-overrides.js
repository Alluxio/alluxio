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
  jest: function (config) {
    config.moduleNameMapper = {
      '^@alluxio/common-ui/(.*)$': resolveApp('../common') + '/$1'
    };
    return config;
  },
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
