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

import { connectRouter, RouterState } from 'connected-react-router';
import { History } from 'history';
import { combineReducers, Reducer } from 'redux';
import { all, AllEffect, fork, ForkEffect } from 'redux-saga/effects';

import { initialRefreshState, refreshReducer } from '@alluxio/common-ui/src/store/refresh/reducer';
import { IRefreshState } from '@alluxio/common-ui/src/store/refresh/types';
import { initialLogsState, logsReducer } from '@alluxio/common-ui/src/store/logs/reducer';
import { logsSaga } from '@alluxio/common-ui/src/store/logs/sagas';
import { ILogsState } from '@alluxio/common-ui/src/store/logs/types';
import { blockInfoReducer, initialBlockInfoState } from './blockInfo/reducer';
import { blockInfoSaga } from './blockInfo/sagas';
import { IBlockInfoState } from './blockInfo/types';
import { initialMetricsState, metricsReducer } from './metrics/reducer';
import { metricsSaga } from './metrics/sagas';
import { IMetricsState } from './metrics/types';
import { initialOverviewState, overviewReducer } from './overview/reducer';
import { overviewSaga } from './overview/sagas';
import { IOverviewState } from './overview/types';
import { IInitState } from './init/types';
import { initialInitState, initReducer } from './init/reducer';
import { initSaga } from './init/sagas';

export interface IApplicationState {
  blockInfo: IBlockInfoState;
  init: IInitState;
  logs: ILogsState;
  metrics: IMetricsState;
  overview: IOverviewState;
  refresh: IRefreshState;
  router?: RouterState;
}

export const rootReducer = (history: History): Reducer<IApplicationState> =>
  combineReducers<IApplicationState>({
    blockInfo: blockInfoReducer,
    init: initReducer,
    logs: logsReducer,
    metrics: metricsReducer,
    overview: overviewReducer,
    refresh: refreshReducer,
    router: connectRouter(history),
  });

export const rootSaga = function*(): IterableIterator<AllEffect<ForkEffect>> {
  yield all([fork(blockInfoSaga), fork(initSaga), fork(logsSaga), fork(metricsSaga), fork(overviewSaga)]);
};

export const initialState: IApplicationState = {
  blockInfo: initialBlockInfoState,
  init: initialInitState,
  logs: initialLogsState,
  metrics: initialMetricsState,
  overview: initialOverviewState,
  refresh: initialRefreshState,
};
