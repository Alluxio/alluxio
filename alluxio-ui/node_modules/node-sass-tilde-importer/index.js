var path = require('path');
var findParentDir = require('find-parent-dir');
var fs = require('fs');

function resolve(targetUrl, source) {
  var packageRoot = findParentDir.sync(source, 'node_modules');

  if (!packageRoot) {
    return null;
  }

  var filePath = path.resolve(packageRoot, 'node_modules', targetUrl);
  var isPotentiallyDirectory = !path.extname(filePath);

  if (isPotentiallyDirectory && fs.existsSync(filePath)) {
    return path.resolve(filePath, 'index');
  } else if (fs.existsSync(path.dirname(filePath))) {
    return filePath;
  }

  return resolve(targetUrl, path.dirname(packageRoot));
}

module.exports = function importer (url, prev, done) {
  return (url[ 0 ] === '~') ? { file: resolve(url.substr(1), prev) } : null;
};
