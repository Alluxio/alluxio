#!/usr/bin/env node

'use strict';

var os = require('os');
var pidtree = require('..');

function help() {
  var help = `  Usage
    $ pidtree <ppid>

  Options
    --list                     To print the pids as a list.

  Examples
    $ pidtree
    $ pidtree --list
    $ pidtree 1
    $ pidtree 1 --list
`;
  console.log(help);
}

function list(ppid) {
  pidtree(ppid === undefined ? -1 : ppid, function(err, list) {
    if (err) {
      console.error(err.message);
      return;
    }
    console.log(list.join(os.EOL));
  });
}

function tree(ppid) {
  pidtree(ppid, {advanced: true}, function(err, list) {
    if (err) {
      console.error(err.message);
      return;
    }

    var parents = {}; // Hash Map of parents
    var tree = {}; // Adiacency Hash Map
    while (list.length > 0) {
      var e = list.pop();
      if (tree[e.ppid]) {
        tree[e.ppid].push(e.pid);
      } else {
        tree[e.ppid] = [e.pid];
      }
      if (ppid === -1) {
        parents[e.pid] = e.ppid;
      }
    }
    var roots = [ppid];
    if (ppid === -1) {
      // Get all the roots
      roots = Object.keys(tree).filter(node => parents[node] === undefined);
    }

    roots.forEach(root => print(tree, root));
  });

  function print(tree, start) {
    function printBranch(node, branch) {
      const isGraphHead = branch.length === 0;
      const children = tree[node] || [];

      let branchHead = '';
      if (!isGraphHead) {
        branchHead = children.length > 0 ? '┬ ' : '─ ';
      }

      console.log(`${branch}${branchHead}${node}`);

      var baseBranch = branch;
      if (!isGraphHead) {
        const isChildOfLastBranch = branch.slice(-2) === '└─';
        baseBranch = branch.slice(0, -2) + (isChildOfLastBranch ? '  ' : '| ');
      }

      const nextBranch = baseBranch + '├─';
      const lastBranch = baseBranch + '└─';
      children.forEach((child, index) => {
        printBranch(
          child,
          children.length - 1 === index ? lastBranch : nextBranch
        );
      });
    }

    printBranch(start, '');
  }
}

function run() {
  var flag;
  var ppid;
  for (var i = 2; i < process.argv.length; i++) {
    if (process.argv[i].startsWith('--')) {
      flag = process.argv[i];
    } else {
      ppid = process.argv[i];
    }
  }
  if (ppid === undefined) {
    ppid = -1;
  }

  if (flag === '--list') list(ppid);
  else if (flag === undefined) tree(ppid);
  else help();
}

run();
