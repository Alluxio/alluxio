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
import { AnyAction, compose, Dispatch } from 'redux';

import { withErrors, withFetchData, withLoadingMessage } from '../';
import { ICommonState, IRequest } from '../../constants';
import { fetchRequest } from '../../store/stacks/actions';
import { Form, FormGroup, Input } from 'reactstrap';
import { History, LocationState } from 'history';

export interface IStackPropsFromState extends ICommonState {
  stackData: string;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IStacksProps {
  history: History<LocationState>;
  location: { search: string };
  queryStringSuffix: string;
  request: IRequest;
}

export type AllStacksProps = IStackPropsFromState & IPropsFromDispatch & IStacksProps;

export class Stacks extends React.Component<AllStacksProps> {
  public render(): JSX.Element {
    const { stackData } = this.props;
    return (
      <div className="col-12">
        <Form className="mb-3 viewData-file-form" id="viewDataFileForm" inline={true}>
          <FormGroup className="mb-2 mr-sm-2 w-100">
            <Input className="w-100" type="textarea" value={stackData} style={{ height: '80vh' }} readOnly={true} />
          </FormGroup>
        </Form>
      </div>
    );
  }
}

export const mapDispatchToStacksProps = (dispatch: Dispatch): { fetchRequest: () => AnyAction } => ({
  fetchRequest: (): AnyAction => dispatch(fetchRequest()),
});

export default compose(
  withFetchData,
  withErrors,
  withLoadingMessage,
)(Stacks) as typeof React.Component;
