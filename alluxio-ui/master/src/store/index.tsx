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

import {connectRouter, RouterState} from 'connected-react-router';
import {History} from 'history';
import {Action, AnyAction, combineReducers, Dispatch} from 'redux';
import {all, fork} from 'redux-saga/effects';

import {refreshReducer} from '@alluxio/common-ui/src/store/refresh/reducer';
import {IRefreshState} from '@alluxio/common-ui/src/store/refresh/types';
import {browseReducer} from './browse/reducer';
import {browseSaga} from './browse/sagas';
import {IBrowseState} from './browse/types';
import {configReducer} from './config/reducer';
import {configSaga} from './config/sagas';
import {IConfigState} from './config/types';
import {dataReducer} from './data/reducer';
import {dataSaga} from './data/sagas';
import {IDataState} from './data/types';
import {logsReducer} from './logs/reducer';
import {logsSaga} from './logs/sagas';
import {ILogsState} from './logs/types';
import {metricsReducer} from './metrics/reducer';
import {metricsSaga} from './metrics/sagas';
import {IMetricsState} from './metrics/types';
import {overviewReducer} from './overview/reducer';
import {overviewSaga} from './overview/sagas';
import {IOverviewState} from './overview/types';
import {workersReducer} from './workers/reducer';
import {workersSaga} from './workers/sagas';
import {IWorkersState} from './workers/types';

export interface IApplicationState {
  browse: IBrowseState;
  config: IConfigState;
  data: IDataState;
  logs: ILogsState;
  metrics: IMetricsState;
  overview: IOverviewState;
  refresh: IRefreshState;
  router: RouterState;
  workers: IWorkersState;
}

export interface IConnectedReduxProps<A extends Action = AnyAction> {
  dispatch: Dispatch<A>;
}

export const rootReducer = (history: History) => combineReducers<IApplicationState>({
  browse: browseReducer,
  config: configReducer,
  data: dataReducer,
  logs: logsReducer,
  metrics: metricsReducer,
  overview: overviewReducer,
  refresh: refreshReducer,
  router: connectRouter(history),
  workers: workersReducer
});

export const rootSaga = function* () {
  yield all([
    fork(browseSaga),
    fork(configSaga),
    fork(dataSaga),
    fork(logsSaga),
    fork(metricsSaga),
    fork(overviewSaga),
    fork(workersSaga)
  ]);
};
