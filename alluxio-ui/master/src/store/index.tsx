import {connectRouter, RouterState} from 'connected-react-router';
import {History} from 'history';
import {Action, AnyAction, combineReducers, Dispatch} from 'redux';
import {all, fork} from 'redux-saga/effects';

import {overviewReducer} from './overview/reducer';
import {overviewSaga} from './overview/sagas';
import {IOverviewState} from './overview/types';

export interface IApplicationState {
  overview: IOverviewState;
  router: RouterState;
}

export interface IConnectedReduxProps<A extends Action = AnyAction> {
  dispatch: Dispatch<A>;
}

export const rootReducer = (history: History) => combineReducers<IApplicationState>({
  overview: overviewReducer,
  router: connectRouter(history)
});

export const rootSaga = function* () {
  yield all([
    fork(overviewSaga)
  ]);
};
