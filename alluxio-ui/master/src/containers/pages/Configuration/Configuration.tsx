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
import {Dispatch} from 'redux';

import {IConfigTriple} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/config/actions';
import {IConfig} from '../../../store/config/types';

interface IPropsFromState {
  data: IConfig;
  errors?: AxiosResponse;
  loading: boolean;
  refresh: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

export class Configuration extends React.Component<AllProps> {
  public componentDidUpdate(prevProps: AllProps) {
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
                    <td><pre className="mb-0"><code>{configuration.left}</code></pre></td>
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

const mapStateToProps = ({config, refresh}: IApplicationState) => ({
  data: config.data,
  errors: config.errors,
  loading: config.loading,
  refresh: refresh.data

});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Configuration);
