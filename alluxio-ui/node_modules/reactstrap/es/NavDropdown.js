import _extends from "@babel/runtime/helpers/esm/extends";
import React from 'react';
import { warnOnce } from './utils';
import Dropdown from './Dropdown';
export default function NavDropdown(props) {
  warnOnce('The "NavDropdown" component has been deprecated.\nPlease use component "Dropdown" with nav prop.');
  return React.createElement(Dropdown, _extends({
    nav: true
  }, props));
}