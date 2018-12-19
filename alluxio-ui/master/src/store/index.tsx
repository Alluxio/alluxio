import {connectRouter, RouterState} from 'connected-react-router';
import {History} from 'history';
import {Action, AnyAction, combineReducers, Dispatch} from 'redux';
import {all, fork} from 'redux-saga/effects';

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
