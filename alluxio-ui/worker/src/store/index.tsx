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
