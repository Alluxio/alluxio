import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaRequest} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {DataActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_data`;

const watchRequest = function* () {
  yield takeLatest(DataActionTypes.FETCH_REQUEST, getSagaRequest('get', API_ENDPOINT, fetchSuccess, fetchError));
};

export const dataSaga = function* () {
  yield all([fork(watchRequest)]);
};
