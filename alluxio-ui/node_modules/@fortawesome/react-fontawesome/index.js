(function (global, factory) {
  typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports, require('@fortawesome/fontawesome-svg-core'), require('prop-types'), require('react')) :
  typeof define === 'function' && define.amd ? define(['exports', '@fortawesome/fontawesome-svg-core', 'prop-types', 'react'], factory) :
  (global = global || self, factory(global['react-fontawesome'] = {}, global.FontAwesome, global.PropTypes, global.React));
}(this, function (exports, fontawesomeSvgCore, PropTypes, React) { 'use strict';

  PropTypes = PropTypes && PropTypes.hasOwnProperty('default') ? PropTypes['default'] : PropTypes;
  React = React && React.hasOwnProperty('default') ? React['default'] : React;

  function _typeof(obj) {
    if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") {
      _typeof = function (obj) {
        return typeof obj;
      };
    } else {
      _typeof = function (obj) {
        return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj;
      };
    }

    return _typeof(obj);
  }

  function _defineProperty(obj, key, value) {
    if (key in obj) {
      Object.defineProperty(obj, key, {
        value: value,
        enumerable: true,
        configurable: true,
        writable: true
      });
    } else {
      obj[key] = value;
    }

    return obj;
  }

  function _objectSpread(target) {
    for (var i = 1; i < arguments.length; i++) {
      var source = arguments[i] != null ? arguments[i] : {};
      var ownKeys = Object.keys(source);

      if (typeof Object.getOwnPropertySymbols === 'function') {
        ownKeys = ownKeys.concat(Object.getOwnPropertySymbols(source).filter(function (sym) {
          return Object.getOwnPropertyDescriptor(source, sym).enumerable;
        }));
      }

      ownKeys.forEach(function (key) {
        _defineProperty(target, key, source[key]);
      });
    }

    return target;
  }

  function _objectWithoutPropertiesLoose(source, excluded) {
    if (source == null) return {};
    var target = {};
    var sourceKeys = Object.keys(source);
    var key, i;

    for (i = 0; i < sourceKeys.length; i++) {
      key = sourceKeys[i];
      if (excluded.indexOf(key) >= 0) continue;
      target[key] = source[key];
    }

    return target;
  }

  function _objectWithoutProperties(source, excluded) {
    if (source == null) return {};

    var target = _objectWithoutPropertiesLoose(source, excluded);

    var key, i;

    if (Object.getOwnPropertySymbols) {
      var sourceSymbolKeys = Object.getOwnPropertySymbols(source);

      for (i = 0; i < sourceSymbolKeys.length; i++) {
        key = sourceSymbolKeys[i];
        if (excluded.indexOf(key) >= 0) continue;
        if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue;
        target[key] = source[key];
      }
    }

    return target;
  }

  function _toConsumableArray(arr) {
    return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _nonIterableSpread();
  }

  function _arrayWithoutHoles(arr) {
    if (Array.isArray(arr)) {
      for (var i = 0, arr2 = new Array(arr.length); i < arr.length; i++) arr2[i] = arr[i];

      return arr2;
    }
  }

  function _iterableToArray(iter) {
    if (Symbol.iterator in Object(iter) || Object.prototype.toString.call(iter) === "[object Arguments]") return Array.from(iter);
  }

  function _nonIterableSpread() {
    throw new TypeError("Invalid attempt to spread non-iterable instance");
  }

  var commonjsGlobal = typeof window !== 'undefined' ? window : typeof global !== 'undefined' ? global : typeof self !== 'undefined' ? self : {};

  function createCommonjsModule(fn, module) {
  	return module = { exports: {} }, fn(module, module.exports), module.exports;
  }

  var humps = createCommonjsModule(function (module) {
  (function(global) {

    var _processKeys = function(convert, obj, options) {
      if(!_isObject(obj) || _isDate(obj) || _isRegExp(obj) || _isBoolean(obj) || _isFunction(obj)) {
        return obj;
      }

      var output,
          i = 0,
          l = 0;

      if(_isArray(obj)) {
        output = [];
        for(l=obj.length; i<l; i++) {
          output.push(_processKeys(convert, obj[i], options));
        }
      }
      else {
        output = {};
        for(var key in obj) {
          if(Object.prototype.hasOwnProperty.call(obj, key)) {
            output[convert(key, options)] = _processKeys(convert, obj[key], options);
          }
        }
      }
      return output;
    };

    // String conversion methods

    var separateWords = function(string, options) {
      options = options || {};
      var separator = options.separator || '_';
      var split = options.split || /(?=[A-Z])/;

      return string.split(split).join(separator);
    };

    var camelize = function(string) {
      if (_isNumerical(string)) {
        return string;
      }
      string = string.replace(/[\-_\s]+(.)?/g, function(match, chr) {
        return chr ? chr.toUpperCase() : '';
      });
      // Ensure 1st char is always lowercase
      return string.substr(0, 1).toLowerCase() + string.substr(1);
    };

    var pascalize = function(string) {
      var camelized = camelize(string);
      // Ensure 1st char is always uppercase
      return camelized.substr(0, 1).toUpperCase() + camelized.substr(1);
    };

    var decamelize = function(string, options) {
      return separateWords(string, options).toLowerCase();
    };

    // Utilities
    // Taken from Underscore.js

    var toString = Object.prototype.toString;

    var _isFunction = function(obj) {
      return typeof(obj) === 'function';
    };
    var _isObject = function(obj) {
      return obj === Object(obj);
    };
    var _isArray = function(obj) {
      return toString.call(obj) == '[object Array]';
    };
    var _isDate = function(obj) {
      return toString.call(obj) == '[object Date]';
    };
    var _isRegExp = function(obj) {
      return toString.call(obj) == '[object RegExp]';
    };
    var _isBoolean = function(obj) {
      return toString.call(obj) == '[object Boolean]';
    };

    // Performant way to determine if obj coerces to a number
    var _isNumerical = function(obj) {
      obj = obj - 0;
      return obj === obj;
    };

    // Sets up function which handles processing keys
    // allowing the convert function to be modified by a callback
    var _processor = function(convert, options) {
      var callback = options && 'process' in options ? options.process : options;

      if(typeof(callback) !== 'function') {
        return convert;
      }

      return function(string, options) {
        return callback(string, convert, options);
      }
    };

    var humps = {
      camelize: camelize,
      decamelize: decamelize,
      pascalize: pascalize,
      depascalize: decamelize,
      camelizeKeys: function(object, options) {
        return _processKeys(_processor(camelize, options), object);
      },
      decamelizeKeys: function(object, options) {
        return _processKeys(_processor(decamelize, options), object, options);
      },
      pascalizeKeys: function(object, options) {
        return _processKeys(_processor(pascalize, options), object);
      },
      depascalizeKeys: function () {
        return this.decamelizeKeys.apply(this, arguments);
      }
    };

    if (module.exports) {
      module.exports = humps;
    } else {
      global.humps = humps;
    }

  })(commonjsGlobal);
  });

  function capitalize(val) {
    return val.charAt(0).toUpperCase() + val.slice(1);
  }

  function styleToObject(style) {
    return style.split(';').map(function (s) {
      return s.trim();
    }).filter(function (s) {
      return s;
    }).reduce(function (acc, pair) {
      var i = pair.indexOf(':');
      var prop = humps.camelize(pair.slice(0, i));
      var value = pair.slice(i + 1).trim();
      prop.startsWith('webkit') ? acc[capitalize(prop)] = value : acc[prop] = value;
      return acc;
    }, {});
  }

  function convert(createElement, element) {
    var extraProps = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};

    if (typeof element === 'string') {
      return element;
    }

    var children = (element.children || []).map(function (child) {
      return convert(createElement, child);
    });
    var mixins = Object.keys(element.attributes || {}).reduce(function (acc, key) {
      var val = element.attributes[key];

      switch (key) {
        case 'class':
          acc.attrs['className'] = val;
          delete element.attributes['class'];
          break;

        case 'style':
          acc.attrs['style'] = styleToObject(val);
          break;

        default:
          if (key.indexOf('aria-') === 0 || key.indexOf('data-') === 0) {
            acc.attrs[key.toLowerCase()] = val;
          } else {
            acc.attrs[humps.camelize(key)] = val;
          }

      }

      return acc;
    }, {
      attrs: {}
    });

    var _extraProps$style = extraProps.style,
        existingStyle = _extraProps$style === void 0 ? {} : _extraProps$style,
        remaining = _objectWithoutProperties(extraProps, ["style"]);

    mixins.attrs['style'] = _objectSpread({}, mixins.attrs['style'], existingStyle);
    return createElement.apply(void 0, [element.tag, _objectSpread({}, mixins.attrs, remaining)].concat(_toConsumableArray(children)));
  }

  var PRODUCTION = false;

  try {
    PRODUCTION = process.env.NODE_ENV === 'production';
  } catch (e) {}

  function log () {
    if (!PRODUCTION && console && typeof console.error === 'function') {
      var _console;

      (_console = console).error.apply(_console, arguments);
    }
  }

  function objectWithKey(key, value) {
    return Array.isArray(value) && value.length > 0 || !Array.isArray(value) && value ? _defineProperty({}, key, value) : {};
  }

  function classList(props) {
    var _classes;

    var classes = (_classes = {
      'fa-spin': props.spin,
      'fa-pulse': props.pulse,
      'fa-fw': props.fixedWidth,
      'fa-inverse': props.inverse,
      'fa-border': props.border,
      'fa-li': props.listItem,
      'fa-flip-horizontal': props.flip === 'horizontal' || props.flip === 'both',
      'fa-flip-vertical': props.flip === 'vertical' || props.flip === 'both'
    }, _defineProperty(_classes, "fa-".concat(props.size), props.size !== null), _defineProperty(_classes, "fa-rotate-".concat(props.rotation), props.rotation !== null), _defineProperty(_classes, "fa-pull-".concat(props.pull), props.pull !== null), _classes);
    return Object.keys(classes).map(function (key) {
      return classes[key] ? key : null;
    }).filter(function (key) {
      return key;
    });
  }

  function normalizeIconArgs(icon) {
    if (icon === null) {
      return null;
    }

    if (_typeof(icon) === 'object' && icon.prefix && icon.iconName) {
      return icon;
    }

    if (Array.isArray(icon) && icon.length === 2) {
      return {
        prefix: icon[0],
        iconName: icon[1]
      };
    }

    if (typeof icon === 'string') {
      return {
        prefix: 'fas',
        iconName: icon
      };
    }
  }

  function FontAwesomeIcon(props) {
    var iconArgs = props.icon,
        maskArgs = props.mask,
        symbol = props.symbol,
        className = props.className,
        title = props.title;
    var iconLookup = normalizeIconArgs(iconArgs);
    var classes = objectWithKey('classes', [].concat(_toConsumableArray(classList(props)), _toConsumableArray(className.split(' '))));
    var transform = objectWithKey('transform', typeof props.transform === 'string' ? fontawesomeSvgCore.parse.transform(props.transform) : props.transform);
    var mask = objectWithKey('mask', normalizeIconArgs(maskArgs));
    var renderedIcon = fontawesomeSvgCore.icon(iconLookup, _objectSpread({}, classes, transform, mask, {
      symbol: symbol,
      title: title
    }));

    if (!renderedIcon) {
      log('Could not find icon', iconLookup);
      return null;
    }

    var abstract = renderedIcon.abstract;
    var extraProps = {};
    Object.keys(props).forEach(function (key) {
      if (!FontAwesomeIcon.defaultProps.hasOwnProperty(key)) {
        extraProps[key] = props[key];
      }
    });
    return convertCurry(abstract[0], extraProps);
  }
  FontAwesomeIcon.displayName = 'FontAwesomeIcon';
  FontAwesomeIcon.propTypes = {
    border: PropTypes.bool,
    className: PropTypes.string,
    mask: PropTypes.oneOfType([PropTypes.object, PropTypes.array, PropTypes.string]),
    fixedWidth: PropTypes.bool,
    inverse: PropTypes.bool,
    flip: PropTypes.oneOf(['horizontal', 'vertical', 'both']),
    icon: PropTypes.oneOfType([PropTypes.object, PropTypes.array, PropTypes.string]),
    listItem: PropTypes.bool,
    pull: PropTypes.oneOf(['right', 'left']),
    pulse: PropTypes.bool,
    rotation: PropTypes.oneOf([90, 180, 270]),
    size: PropTypes.oneOf(['lg', 'xs', 'sm', '1x', '2x', '3x', '4x', '5x', '6x', '7x', '8x', '9x', '10x']),
    spin: PropTypes.bool,
    symbol: PropTypes.oneOfType([PropTypes.bool, PropTypes.string]),
    title: PropTypes.string,
    transform: PropTypes.oneOfType([PropTypes.string, PropTypes.object])
  };
  FontAwesomeIcon.defaultProps = {
    border: false,
    className: '',
    mask: null,
    fixedWidth: false,
    inverse: false,
    flip: null,
    icon: null,
    listItem: false,
    pull: null,
    pulse: false,
    rotation: null,
    size: null,
    spin: false,
    symbol: false,
    title: '',
    transform: null
  };
  var convertCurry = convert.bind(null, React.createElement);

  exports.FontAwesomeIcon = FontAwesomeIcon;

  Object.defineProperty(exports, '__esModule', { value: true });

}));
