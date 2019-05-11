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

import {INavigationData} from '@alluxio/common-ui/src/constants';

export const headerNavigationData : INavigationData[] = [{
  innerText: 'Overview',
  url: '/overview'
}, {
  innerText: 'Browse',
  url: '/browse'
}, {
  innerText: 'In-Alluxio Data',
  url: '/data'
}, {
  innerText: 'Logs',
  url: '/logs'
}, {
  innerText: 'Configuration',
  url: '/config'
}, {
  innerText: 'Workers',
  url: '/workers'
}, {
  innerText: 'Metrics',
  url: '/metrics'
}];
