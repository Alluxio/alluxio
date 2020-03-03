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

export interface IFluidContainerProps {
  class: string;
}

export function withFluidContainer<T extends IFluidContainerProps>(
  WrappedComponent: React.ComponentType<T>,
): React.ComponentType<T> {
  class FluidContainerHoc extends React.Component<T> {
    render(): JSX.Element {
      return (
        <div className={this.props.class}>
          <div className="container-fluid">
            <div className="row">
              <WrappedComponent {...this.props} />
            </div>
          </div>
        </div>
      );
    }
  }
  (FluidContainerHoc as React.ComponentType<T>).displayName = `withFluidContainer(${getDisplayName(WrappedComponent)})`;
  return FluidContainerHoc;
}
