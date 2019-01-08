import {clearTimeout, setTimeout} from 'timers';

export const getDebouncedFunction = (fn: () => void, delay: number, leadingAndEnding: boolean = false) => {
  let timeout: any;
  let args: any[];
  let timeNow: number;
  let lastCallTime: number;

  // tslint:disable-next-line:only-arrow-functions
  return function(this: typeof fn) {
    args = [].slice.call(arguments);
    const callFunction = function(this: typeof fn) {
      fn.apply(this, args)
    };

    clearTimeout(timeout);
    timeout = setTimeout(callFunction, delay);

    if (leadingAndEnding) {
      timeNow = (new Date()).getTime();
      if (!lastCallTime || timeNow > lastCallTime + delay) {
        lastCallTime = (new Date()).getTime();
        callFunction.call(this);
      }
      return;
    }
  };
};
