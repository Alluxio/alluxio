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

import React from 'react';
import {connect} from 'react-redux';

import {AllProps, Logs} from '@alluxio/common-ui/src/components';
import {IApplicationState} from '../../../store';
import {Dispatch} from "redux";
import {fetchRequest} from "@alluxio/common-ui/src/store/logs/actions";

export const WorkerLogs = (props: AllProps) => <Logs {...props} />;

const mapStateToProps = ({logs, refresh}: IApplicationState) => ({
  data: logs.data,
  errors: logs.errors,
  loading: logs.loading,
  refresh: refresh.data
});

export const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
    mapStateToProps,
    mapDispatchToProps
)(WorkerLogs);
