import {all, fork, takeLatest} from 'redux-saga/effects';

import {getSagaFetchGenerator} from '@alluxio/common-ui/src/utilities';
import {fetchError, fetchSuccess} from './actions';
import {BrowseActionTypes} from './types';

const API_ENDPOINT = `${process.env.REACT_APP_API_ROOT}/webui_browse`;

const watchRequest = function* () {
  yield takeLatest(BrowseActionTypes.FETCH_REQUEST, getSagaFetchGenerator(API_ENDPOINT, fetchSuccess, fetchError));
};

export const browseSaga = function* () {
  yield all([fork(watchRequest)]);
};
