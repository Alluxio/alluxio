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
import { LoadingMessage } from '../..';

export interface ILoadingProps {
  loading: boolean;
  class: string;
}

export function withLoadingMessage<T extends ILoadingProps>(
  WrappedComponent: React.ComponentType<T>,
): React.ComponentType<T> {
  class LoaderHoc extends React.Component<T> {
    render(): JSX.Element {
      return this.props.loading ? (
        <div className={this.props.class}>
          <LoadingMessage />
        </div>
      ) : (
        <WrappedComponent {...this.props} />
      );
    }
  }
  (LoaderHoc as React.ComponentType<T>).displayName = `withLoadingMessage(${getDisplayName(WrappedComponent)})`;
  return LoaderHoc;
}
