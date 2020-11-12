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
import { browseReducer, initialBrowseState } from './browse/reducer';
import { browseSaga } from './browse/sagas';
import { IBrowseState } from './browse/types';
import { configReducer, initialConfigState } from './config/reducer';
import { configSaga } from './config/sagas';
import { IConfigState } from './config/types';
import { dataReducer, initialDataState } from './data/reducer';
import { dataSaga } from './data/sagas';
import { IDataState } from './data/types';
import { initialInitState, initReducer } from './init/reducer';
import { initSaga } from './init/sagas';
import { IInitState } from './init/types';
import { initialMetricsState, metricsReducer } from './metrics/reducer';
import { metricsSaga } from './metrics/sagas';
import { IMetricsState } from './metrics/types';
import { initialOverviewState, overviewReducer } from './overview/reducer';
import { overviewSaga } from './overview/sagas';
import { IOverviewState } from './overview/types';
import { initialWorkersState, workersReducer } from './workers/reducer';
import { workersSaga } from './workers/sagas';
import { IWorkersState } from './workers/types';
import { IMountTableState } from './mounttable/types';
import { initialMountTableState, mountTableReducer } from './mounttable/reducer';
import { mountTableSaga } from './mounttable/sagas';

export interface IApplicationState {
  browse: IBrowseState;
  config: IConfigState;
  data: IDataState;
  init: IInitState;
  logs: ILogsState;
  metrics: IMetricsState;
  overview: IOverviewState;
  refresh: IRefreshState;
  router?: RouterState;
  workers: IWorkersState;
  mountTable: IMountTableState;
}

export const rootReducer = (history: History): Reducer<IApplicationState> =>
  combineReducers<IApplicationState>({
    browse: browseReducer,
    config: configReducer,
    data: dataReducer,
    init: initReducer,
    logs: logsReducer,
    metrics: metricsReducer,
    overview: overviewReducer,
    refresh: refreshReducer,
    router: connectRouter(history),
    workers: workersReducer,
    mountTable: mountTableReducer,
  });

export const rootSaga = function*(): IterableIterator<AllEffect<ForkEffect>> {
  yield all([
    fork(browseSaga),
    fork(configSaga),
    fork(dataSaga),
    fork(initSaga),
    fork(logsSaga),
    fork(metricsSaga),
    fork(overviewSaga),
    fork(workersSaga),
    fork(mountTableSaga),
  ]);
};

export const initialState: IApplicationState = {
  browse: initialBrowseState,
  config: initialConfigState,
  data: initialDataState,
  init: initialInitState,
  logs: initialLogsState,
  metrics: initialMetricsState,
  overview: initialOverviewState,
  refresh: initialRefreshState,
  workers: initialWorkersState,
  mountTable: initialMountTableState,
};
