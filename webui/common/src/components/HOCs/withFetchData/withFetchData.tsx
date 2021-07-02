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
import { getDisplayName } from '../../../utilities';

export interface IFetchDataProps {
  refresh: boolean;
  fetchRequest: () => void;
}

export function withFetchData<T extends IFetchDataProps>(
  WrappedComponent: React.ComponentType<T>,
): React.ComponentType<T> {
  class FetchDataHoc extends React.Component<T> {
    public componentDidUpdate(prevProps: T): void {
      if (this.props.refresh !== prevProps.refresh) {
        this.props.fetchRequest();
      }
    }

    public componentWillMount(): void {
      this.props.fetchRequest();
    }

    public render(): JSX.Element {
      return <WrappedComponent {...this.props} />;
    }
  }
  (FetchDataHoc as React.ComponentType<T>).displayName = `withFetchData(${getDisplayName(WrappedComponent)})`;
  return FetchDataHoc;
}
