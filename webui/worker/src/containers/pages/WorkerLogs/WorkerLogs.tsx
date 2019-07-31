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
import {
  AllProps,
  getLogPropsFromState,
  mapDispatchToLogProps
} from '@alluxio/common-ui/src/components';
import {IApplicationState} from '../../../store';
import Logs from "@alluxio/common-ui/src/components/Logs/Logs";
import {ILogsStateToProps} from "@alluxio/common-ui/src/store/logs/types";

const WorkerLogs = (props: AllProps) => <Logs {...props} />;

const mapStateToProps = ({logs, refresh}: IApplicationState): ILogsStateToProps => getLogPropsFromState(logs, refresh);

export default connect(
    mapStateToProps,
    mapDispatchToLogProps
)(WorkerLogs);
