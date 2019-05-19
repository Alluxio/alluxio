import React from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';
import { mapToCssModules } from './utils';

var CarouselControl = function CarouselControl(props) {
  var direction = props.direction,
      onClickHandler = props.onClickHandler,
      cssModule = props.cssModule,
      directionText = props.directionText,
      className = props.className;
  var anchorClasses = mapToCssModules(classNames(className, "carousel-control-" + direction), cssModule);
  var iconClasses = mapToCssModules(classNames("carousel-control-" + direction + "-icon"), cssModule);
  var screenReaderClasses = mapToCssModules(classNames('sr-only'), cssModule);
  return React.createElement("a", {
    className: anchorClasses,
    role: "button",
    tabIndex: "0",
    onClick: function onClick(e) {
      e.preventDefault();
      onClickHandler();
    }
  }, React.createElement("span", {
    className: iconClasses,
    "aria-hidden": "true"
  }), React.createElement("span", {
    className: screenReaderClasses
  }, directionText || direction));
};

CarouselControl.propTypes = {
  direction: PropTypes.oneOf(['prev', 'next']).isRequired,
  onClickHandler: PropTypes.func.isRequired,
  cssModule: PropTypes.object,
  directionText: PropTypes.string,
  className: PropTypes.string
};
export default CarouselControl;