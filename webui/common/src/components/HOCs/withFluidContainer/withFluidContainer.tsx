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
import {getDisplayName} from "../../../utilities";

export interface IFluidContainerProps {
    class: string;
}

export function withFluidContainer<T extends IFluidContainerProps>(WrappedComponent: React.ComponentType<T>) {
    const fluidContainerHoc = (props: T) => {
        return (
            <div className={props.class}>
                <div className="container-fluid">
                    <div className="row">
                        <WrappedComponent {...props} />
                    </div>
                </div>
            </div>
        )
    };
    (fluidContainerHoc as React.FunctionComponent).displayName = `withFluidContainer(${getDisplayName(WrappedComponent)})`;
    return fluidContainerHoc;
}
