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

import { triggerRefresh } from '../../store/refresh/actions';

export interface IAutoRefresh {
  setAutoRefresh: (should: boolean) => void;
}

export class AutoRefresh implements IAutoRefresh {
  private intervalHandle: NodeJS.Timeout | null = null;
  private readonly refreshCallback: typeof triggerRefresh;
  private readonly interval: number;

  constructor(callback: typeof triggerRefresh, interval: number) {
    this.setAutoRefresh = this.setAutoRefresh.bind(this);
    this.refreshCallback = callback;
    this.interval = interval;
  }

  public setAutoRefresh(enable: boolean): void {
    if (enable && !this.intervalHandle) {
      this.intervalHandle = setInterval(this.refreshCallback, this.interval);
    } else if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }
  }
}
