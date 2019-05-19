/* global jest */

/* eslint-disable import/no-extraneous-dependencies */
import Enzyme, { ReactWrapper } from 'enzyme';
import Adapter from 'enzyme-adapter-react-16';
Enzyme.configure({
  adapter: new Adapter()
}); // TODO remove when enzyme releases https://github.com/airbnb/enzyme/pull/1179

ReactWrapper.prototype.hostNodes = function () {
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