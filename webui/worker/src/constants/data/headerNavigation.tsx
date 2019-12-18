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

import { INavigationData } from '@alluxio/common-ui/src/constants';
import { routePaths } from './routePaths';

export const headerNavigationData: INavigationData[] = [
  {
    innerText: 'Overview',
    url: routePaths.overview,
  },
  {
    innerText: 'BlockInfo',
    url: routePaths.blockInfo,
  },
  {
    innerText: 'Logs',
    url: routePaths.logs,
  },
  {
    innerText: 'Metrics',
    url: routePaths.metrics,
  },
  {
    innerText: 'Return to Master',
    url: ({ masterHostname, masterPort }): string => `${window.location.protocol}//${masterHostname}:${masterPort}`,
  },
];
