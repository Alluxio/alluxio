import _extends from "@babel/runtime/helpers/esm/extends";
import React from 'react';
import PropTypes from 'prop-types';
import Dropdown from './Dropdown';
var propTypes = {
  children: PropTypes.node
};

var ButtonDropdown = function ButtonDropdown(props) {
  return React.createElement(Dropdown, _extends({
    group: true
  }, props));
};

ButtonDropdown.propTypes = propTypes;
export default ButtonDropdown;