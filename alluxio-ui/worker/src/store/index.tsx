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

import {blockInfoReducer} from './blockInfo/reducer';
import {blockInfoSaga} from './blockInfo/sagas';
import {IBlockInfoState} from './blockInfo/types';
import {logsReducer} from './logs/reducer';
import {logsSaga} from './logs/sagas';
import {ILogsState} from './logs/types';
import {metricsReducer} from './metrics/reducer';
import {metricsSaga} from './metrics/sagas';
import {IMetricsState} from './metrics/types';
import {overviewReducer} from './overview/reducer';
import {overviewSaga} from './overview/sagas';
import {IOverviewState} from './overview/types';

export interface IApplicationState {
  blockInfo: IBlockInfoState
  logs: ILogsState;
  metrics: IMetricsState;
  overview: IOverviewState;
  router: RouterState;
}

export interface IConnectedReduxProps<A extends Action = AnyAction> {
  dispatch: Dispatch<A>;
}

export const rootReducer = (history: History) => combineReducers<IApplicationState>({
  blockInfo: blockInfoReducer,
  logs: logsReducer,
  metrics: metricsReducer,
  overview: overviewReducer,
  router: connectRouter(history)
});

export const rootSaga = function* () {
  yield all([
    fork(blockInfoSaga),
    fork(logsSaga),
    fork(metricsSaga),
    fork(overviewSaga)
  ]);
};
