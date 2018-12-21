import {INavigationData} from '@alluxio/common-ui/src/constants';

export const headerNavigationData : INavigationData[] = [{
  innerText: 'Overview',
  url: '/overview'
}, {
  innerText: 'BlockInfo',
  url: '/blockInfo'
}, {
  innerText: 'Logs',
  url: '/logs'
}, {
  innerText: 'Metrics',
  url: '/metrics'
}, {
  innerText: 'Return to Master',
  url: () => `${window.location.origin.replace(/\:[0-9]+$/, ':19999')}`
}];
