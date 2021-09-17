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
import { Input, Table } from 'reactstrap';
import { AnyAction, compose, Dispatch } from 'redux';

import { withErrors, withLoadingMessage, withFetchData } from '@alluxio/common-ui/src/components';
import { IConfigTriple } from '../../../constants';
import { IApplicationState } from '../../../store';
import { fetchRequest } from '../../../store/config/actions';
import { IConfig } from '../../../store/config/types';
import { IAlertErrors, ICommonState } from '@alluxio/common-ui/src/constants';
import { createAlertErrors } from '@alluxio/common-ui/src/utilities';
import './Configuration.scss';

interface IPropsFromState extends ICommonState {
  data: IConfig;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

export type AllProps = IPropsFromState & IPropsFromDispatch;

interface IState {
  searchConfig: string;
}

export class ConfigurationPresenter extends React.Component<AllProps, IState> {
  constructor(props: AllProps) {
    super(props);
    this.__searchInputHandler.bind(this);
    this.state = {
      searchConfig: '',
    };
  }

  public render(): JSX.Element {
    const { searchConfig } = this.state;
    const { data } = this.props;
    const filteredData = data.configuration.filter((data: IConfigTriple) =>
      data.left.toLowerCase().includes(searchConfig.toLowerCase()),
    );
    return (
      <div className="configuration-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-6">
              <h5>Alluxio Configuration</h5>
            </div>
            <div className=" search-container col-6">
              <Input
                type="text"
                id="searchConfig"
                placeholder="Search by Property"
                value={searchConfig}
                onChange={this.__searchInputHandler}
              />
            </div>
            <div className="col-12">
              <Table hover={true}>
                <thead>
                  <tr>
                    <th>Property</th>
                    <th>Value</th>
                    <th>Source</th>
                  </tr>
                </thead>
                {filteredData.length ? (
                  <tbody id="filtered-data-body">
                    {filteredData.map((configuration: IConfigTriple) => (
                      <tr key={configuration.left}>
                        <td>
                          <pre className="mb-0">
                            <code>{configuration.left}</code>
                          </pre>
                        </td>
                        <td>{configuration.middle}</td>
                        <td>{configuration.right}</td>
                      </tr>
                    ))}
                  </tbody>
                ) : (
                  <tbody id="no-data-body">
                    <tr>
                      <td> Sorry, no matching properties. </td>
                    </tr>
                  </tbody>
                )}
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

  private __searchInputHandler = (e: React.ChangeEvent<HTMLInputElement>): void => {
    this.setState({ searchConfig: e.target.value.trim() });
  };
}

const mapStateToProps = ({ config, refresh }: IApplicationState): IPropsFromState => {
  const errors: IAlertErrors = createAlertErrors(config.errors !== undefined, []);
  return {
    data: config.data,
    errors: errors,
    loading: config.loading,
    refresh: refresh.data,
    class: 'configuration-page',
  };
};

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
)(ConfigurationPresenter) as typeof React.Component;
