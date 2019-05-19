'use strict';
const pReduce = require('p-reduce');

module.exports = (iterable, initVal) => pReduce(iterable, (prev, fn) => fn(prev), initVal);
