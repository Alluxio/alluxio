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

import {AxiosResponse} from 'axios';
import React from 'react';
import {connect} from 'react-redux';
import {Alert, Progress, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {IScopedPropertyInfo, IStorageTierInfo} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/overview/actions';
import {IOverview} from '../../../store/overview/types';

interface IPropsFromState {
  data: IOverview;
  errors?: AxiosResponse;
  loading: boolean;
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Overview extends React.Component<AllProps> {
  public componentWillUpdate(prevProps: AllProps) {
    if (this.props.refresh !== prevProps.refresh) {
      this.props.fetchRequest();
    }
  }

  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {errors, data} = this.props;

    if (errors) {
      return (
        <Alert color="danger">
          Unable to reach the api endpoint for this page.
        </Alert>
      );
    }

    return (
      <div className="overview-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-6">
              <h5>Alluxio Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Master Address</th>
                  <td>{data.masterNodeAddress}</td>
                </tr>
                <tr>
                  <th scope="row">Started</th>
                  <td>{data.startTime}</td>
                </tr>
                <tr>
                  <th scope="row">Uptime</th>
                  <td>{data.uptime}</td>
                </tr>
                <tr>
                  <th scope="row">Version</th>
                  <td>{data.version}</td>
                </tr>
                <tr>
                  <th scope="row">Running Workers</th>
                  <td>{data.liveWorkerNodes}</td>
                </tr>
                <tr>
                  <th scope="row">Startup Consistency Check</th>
                  <td>{data.consistencyCheckStatus}</td>
                </tr>
                {this.renderInconsistentPaths(data.inconsistentPathItems)}
                <tr>
                  <th scope="row">Server Configuration Check</th>
                  <td className={data.configCheckStatus === 'FAILED' ? 'text-danger' : ''}>
                    {data.configCheckStatus}
                  </td>
                </tr>
                {this.renderConfigurationIssues(data.configCheckErrors, 'text-error')}
                {this.renderConfigurationIssues(data.configCheckWarns, 'text-warning')}
                </tbody>
              </Table>
            </div>
            <div className="col-md-6">
              <h5>Cluster Usage Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Workers Capacity</th>
                  <td>{data.capacity}</td>
                </tr>
                <tr>
                  <th scope="row">Workers Free / Used</th>
                  <td>{data.freeCapacity} / {data.usedCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">UnderFS Capacity</th>
                  <td>{data.diskCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">UnderFS Free / Used</th>
                  <td>{data.diskFreeCapacity} / {data.diskUsedCapacity}</td>
                </tr>
                </tbody>
              </Table>
            </div>
            <div className="col-md-12">
              <h5>Storage Usage Summary</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Storage Alias</th>
                  <th>Space Capacity</th>
                  <th>Space Used</th>
                  <th>Space Usage</th>
                </tr>
                </thead>
                <tbody>
                {data.storageTierInfos.map((info: IStorageTierInfo) => (
                  <tr key={info.storageTierAlias}>
                    <td>{info.storageTierAlias}</td>
                    <td>{info.capacity}</td>
                    <td>{info.usedCapacity}</td>
                    <td>
                      <Progress multi={true}>
                        <Progress bar={true} color="success" value={`${info.freeSpacePercent}`}>{info.freeSpacePercent}%
                          Free</Progress>
                        <Progress bar={true} color="danger" value={`${info.usedSpacePercent}`}>{info.usedSpacePercent}%
                          Used</Progress>
                      </Progress>
                    </td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderInconsistentPaths(paths: string[]) {
    if (!paths || !paths.length) {
      return null;
    }

    return (
      <tr>
        <th scope="row" className="text-danger">Inconsistent Files on Startup (run fs checkConsistency for details)</th>
        <td>
          {paths.map((path: string) => <div key={path} className="text-danger">{path}</div>)}
        </td>
      </tr>
    );
  }

  private renderConfigurationIssues(issues: IScopedPropertyInfo[], className: string) {
    if (!issues || !issues.length) {
      return null;
    }

    const errorMarkup = issues.map((issue: IScopedPropertyInfo) => {
      let markup = '';
      for (const scope of Object.keys(issue)) {
        const scopeValue = issue[scope];
        markup += `<div>${scope}`;
        for (const property of Object.keys(scopeValue)) {
          const propertyValue = scopeValue[property];
          markup += `<div>${property}: ${propertyValue}</div>`;
        }
        markup += '</div>';
      }
      return markup;
    });

    return (
      <tr>
        <th scope="row" className={className}>Inconsistent Properties</th>
        <td className={className}>
          {errorMarkup}
        </td>
      </tr>
    )
  }
}

const mapStateToProps = ({overview, refresh}: IApplicationState) => ({
  data: overview.data,
  errors: overview.errors,
  loading: overview.loading,
  refresh: refresh.data
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Overview);
