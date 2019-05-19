import React from 'react';
import PropTypes from 'prop-types';
import Dropdown from './Dropdown';
var propTypes = {
  addonType: PropTypes.oneOf(['prepend', 'append']).isRequired,
  children: PropTypes.node
};

var InputGroupButtonDropdown = function InputGroupButtonDropdown(props) {
  return React.createElement(Dropdown, props);
};

InputGroupButtonDropdown.propTypes = propTypes;
export default InputGroupButtonDropdown;