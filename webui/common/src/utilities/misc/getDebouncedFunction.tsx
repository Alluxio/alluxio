/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

import { clearTimeout, setTimeout } from 'timers';

export const getDebouncedFunction = (
  fn: Function,
  delay: number,
  leadingAndEnding = false,
): ((this: Function, ...args: []) => void) => {
  let timeout: NodeJS.Timeout;
  let timeNow: number;
  let lastCallTime: number;

  return function(this: Function, ...argList: []): void {
    const callFunction = function(this: typeof fn): void {
      fn.apply(this, [].slice.call(argList) as []);
    };

    clearTimeout(timeout);
    timeout = setTimeout(callFunction, delay);

    if (leadingAndEnding) {
      timeNow = new Date().getTime();
      if (!lastCallTime || timeNow > lastCallTime + delay) {
        lastCallTime = new Date().getTime();
        callFunction.call(this);
      }
      return;
    }
  };
};
