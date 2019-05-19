import _extends from "@babel/runtime/helpers/esm/extends";
import React from 'react';
import { warnOnce } from './utils';
import UncontrolledDropdown from './UncontrolledDropdown';

var UncontrolledNavDropdown = function UncontrolledNavDropdown(props) {
  warnOnce('The "UncontrolledNavDropdown" component has been deprecated.\nPlease use component "UncontrolledDropdown" with nav prop.');
  return React.createElement(UncontrolledDropdown, _extends({
    nav: true
  }, props));
};

export default UncontrolledNavDropdown;