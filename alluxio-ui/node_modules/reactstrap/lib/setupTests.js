"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _enzyme = _interopRequireWildcard(require("enzyme"));

var _enzymeAdapterReact = _interopRequireDefault(require("enzyme-adapter-react-16"));

/* global jest */

/* eslint-disable import/no-extraneous-dependencies */
_enzyme.default.configure({
  adapter: new _enzymeAdapterReact.default()
}); // TODO remove when enzyme releases https://github.com/airbnb/enzyme/pull/1179


_enzyme.ReactWrapper.prototype.hostNodes = function () {
  return this.filterWhere(function (n) {
    return typeof n.type() === 'string';
  });
};

global.requestAnimationFrame = function (cb) {
  cb(0);
};

global.window.cancelAnimationFrame = function () {};

global.createSpyObj = function (baseName, methodNames) {
  var obj = {};

  for (var i = 0; i < methodNames.length; i += 1) {
    obj[methodNames[i]] = jest.fn();
  }

  return obj;
};

global.document.createRange = function () {
  return {
    setStart: function setStart() {},
    setEnd: function setEnd() {},
    commonAncestorContainer: {}
  };
};