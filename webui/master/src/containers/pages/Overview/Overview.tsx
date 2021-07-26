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
import { connect } from 'react-redux';
import { Progress, Table } from 'reactstrap';
import { AnyAction, compose, Dispatch } from 'redux';

import { withErrors, withFluidContainer, withLoadingMessage, withFetchData } from '@alluxio/common-ui/src/components';
import { IScopedPropertyInfo, IStorageTierInfo } from '../../../constants';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/overview/actions';
import { IOverview } from '../../../store/overview/types';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import { ICommonState } from '@alluxio/common-ui/src/constants';

interface IPropsFromState extends ICommonState {
  data: IOverview;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class OverviewPresenter extends React.Component<AllProps> {
  public render(): JSX.Element {
    const { data } = this.props;
    return (
      <React.Fragment>
        <div className="col-md-6">
          <h5>Alluxio Summary</h5>
          <Table hover={true}>
            <tbody>
              <tr>
                <th scope="row">Master Address</th>
                <td>{data.masterNodeAddress}</td>
              </tr>
              <tr>
                <th scope="row">Cluster Id</th>
                <td>{data.clusterId}</td>
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
                <th scope="row">Server Configuration Check</th>
                <td className={data.configCheckStatus === 'FAILED' ? 'text-danger' : ''}>{data.configCheckStatus}</td>
              </tr>
              {this.renderConfigurationIssues(data.configCheckErrors, 'text-error')}
              {this.renderConfigurationIssues(data.configCheckWarns, 'text-warning')}
              {this.renderJournalDiskWarnings(data.journalDiskWarnings, 'text-warning')}
              {this.renderJournalCheckpointWarning(data.journalCheckpointTimeWarning, 'text-warning')}
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
                <td>
                  {data.freeCapacity} / {data.usedCapacity}
                </td>
              </tr>
              <tr>
                <th scope="row">UnderFS Capacity</th>
                <td>{data.diskCapacity}</td>
              </tr>
              <tr>
                <th scope="row">UnderFS Free / Used</th>
                <td>
                  {data.diskFreeCapacity} / {data.diskUsedCapacity}
                </td>
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
                    <Progress className="h-50 mt-1" multi={true}>
                      <Progress bar={true} color="dark" value={`${info.freeSpacePercent}`}>
                        {info.freeSpacePercent}% Free
                      </Progress>
                      <Progress bar={true} color="secondary" value={`${info.usedSpacePercent}`}>
                        {info.usedSpacePercent}% Used
                      </Progress>
                    </Progress>
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        </div>
      </React.Fragment>
    );
  }

  private renderJournalCheckpointWarning(warning: string, className: string): JSX.Element | null {
    if (!warning || warning == '') {
      return null;
    }

    return (
      <tr key="journal-ckpt-warning-0">
        <td colSpan={2} className={className}>
          {warning}
        </td>
      </tr>
    );
  }

  private renderJournalDiskWarnings(warnings: string[], className: string): JSX.Element | null {
    if (!warnings || !warnings.length) {
      return null;
    }

    const warningRender = warnings.map((warning: string, idx: number) => (
      <tr key={idx}>
        <td colSpan={2} className={className}>
          {warning}
        </td>
      </tr>
    ));
    const header = (
      <tr key="-1">
        <th colSpan={2} scope="row" className={className}>
          Journal Disk Warning{warnings.length > 1 ? 's' : ''}
        </th>
      </tr>
    );
    warningRender.unshift(header);

    return <React.Fragment>{warningRender}</React.Fragment>;
  }

  private renderConfigurationIssues(issues: IScopedPropertyInfo[], className: string): JSX.Element | null {
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
        <th scope="row" className={className}>
          Inconsistent Properties
        </th>
        <td className={className}>{errorMarkup}</td>
      </tr>
    );
  }
}

const mapStateToProps = ({ overview, refresh }: IApplicationState): IPropsFromState => ({
  data: overview.data,
  errors: createAlertErrors(overview.errors !== undefined),
  loading: overview.loading,
  refresh: refresh.data,
  class: 'overview-page',
});

const mapDispatchToProps = (dispatch: Dispatch): { fetchRequest: () => AnyAction } => ({
  fetchRequest: (): AnyAction => dispatch(fetchRequest()),
});

export default compose(
  connect(
    mapStateToProps,
    mapDispatchToProps,
  ),
  withFetchData,
  withErrors,
  withLoadingMessage,
  withFluidContainer,
)(OverviewPresenter) as typeof React.Component;
