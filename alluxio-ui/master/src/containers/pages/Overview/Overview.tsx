import React from 'react';
import {connect} from 'react-redux';
import {Progress, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/overview/actions';
import {IOverview, IOverviewScopedPropertyIssue, IOverviewStorageTierInfo} from '../../../store/overview/types';

import './Overview.css';

interface IPropsFromState {
  errors: string;
  loading: boolean;
  overview: IOverview;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Overview extends React.Component<AllProps> {
  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {loading, overview} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <div className="overview-page">
        <div className="conteiner-fluid">
          <div className="row">
            <div className="col-md-6">
              <h5>Alluxio Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Master Address</th>
                  <td>{overview.masterNodeAddress}</td>
                </tr>
                <tr>
                  <th scope="row">Started</th>
                  <td>{overview.startTime}</td>
                </tr>
                <tr>
                  <th scope="row">Uptime</th>
                  <td>{overview.uptime}</td>
                </tr>
                <tr>
                  <th scope="row">Version</th>
                  <td>{overview.version}</td>
                </tr>
                <tr>
                  <th scope="row">Running Workers</th>
                  <td>{overview.liveWorkerNodes}</td>
                </tr>
                <tr>
                  <th scope="row">Startup Consistency Check</th>
                  <td>{overview.consistencyCheckStatus}</td>
                </tr>
                {this.renderInconsistendPaths(overview.inconsistentPathItems)}
                <tr>
                  <th scope="row">Server Configuration Check</th>
                  <td className={overview.configCheckStatus === 'FAILED' ? 'text-danger' : ''}>
                    {overview.configCheckStatus}
                  </td>
                </tr>
                {this.renderConfigurationIssues(overview.configCheckErrors, 'text-error')}
                {this.renderConfigurationIssues(overview.configCheckWarns, 'text-warning')}
                </tbody>
              </Table>
            </div>
            <div className="col-md-6">
              <h5>Cluster Usage Summary</h5>
              <Table hover={true}>
                <tbody>
                <tr>
                  <th scope="row">Workers Capacity</th>
                  <td>{overview.freeCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">Workers Free / Used</th>
                  <td>{overview.freeCapacity} / {overview.usedCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">UnderFS Capacity</th>
                  <td>{overview.diskCapacity}</td>
                </tr>
                <tr>
                  <th scope="row">UnderFS Free / Used</th>
                  <td>{overview.diskCapacity} / {overview.diskUsedCapacity}</td>
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
                {this.renderStorageTierInfos(overview.storageTierInfos)}
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderInconsistendPaths(paths: string[]) {
    if (!paths.length) {
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

  private renderConfigurationIssues(issues: IOverviewScopedPropertyIssue[], className: string) {
    if (!issues.length) {
      return null;
    }

    const errorMarkup = issues.map((issue: IOverviewScopedPropertyIssue) => {
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

  private renderStorageTierInfos(infos: IOverviewStorageTierInfo[]) {
    return infos.map((info: IOverviewStorageTierInfo) => (
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
    ));
  }
}

const mapStateToProps = ({overview}: IApplicationState) => ({
  errors: overview.errors,
  loading: overview.loading,
  overview: overview.overview
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Overview);
