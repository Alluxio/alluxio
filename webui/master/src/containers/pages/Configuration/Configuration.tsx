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
import {Alert, Table} from 'reactstrap';
import {compose, Dispatch} from 'redux';

import {hasErrors, hasLoader, LoadingMessage} from '@alluxio/common-ui/src/components';
import {IConfigTriple} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/config/actions';
import {IConfig, IConfigStateToProps} from '../../../store/config/types';
import {hasFetchData, IFetchDataProps} from "@alluxio/common-ui/src/components/HOCs/hasFetchData";
import {IAlertErrors} from "@alluxio/common-ui/src/constants";
import {createAlertErrors} from "@alluxio/common-ui/src/utilities";

interface IPropsFromState {
  data: IConfig;
}

export type AllProps = IPropsFromState;

export class ConfigurationPresenter extends React.Component<AllProps> {
  public render() {
    const {data} = this.props;

    return (
      <div className="configuration-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Alluxio Configuration</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Property</th>
                  <th>Value</th>
                  <th>Source</th>
                </tr>
                </thead>
                <tbody>
                {data.configuration.map((configuration: IConfigTriple) => (
                  <tr key={configuration.left}>
                    <td>
                      <pre className="mb-0"><code>{configuration.left}</code></pre>
                    </td>
                    <td>{configuration.middle}</td>
                    <td>{configuration.right}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Whitelist</h5>
              <Table hover={true}>
                <tbody>
                {data.whitelist.map((whitelist: string) => (
                  <tr key={whitelist}>
                    <td>{whitelist}</td>
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
}

const mapStateToProps = ({config, refresh}: IApplicationState): IConfigStateToProps => {
  const errors: IAlertErrors = createAlertErrors(config.errors !== undefined, []);
  return {
    data: config.data,
    errors: errors,
    loading: config.loading,
    refresh: refresh.data,
    class: 'configuration-page'
  }

};

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default compose(
    connect(mapStateToProps, mapDispatchToProps),
    hasFetchData,
    hasErrors,
    hasLoader
)(ConfigurationPresenter) as React.Component;
