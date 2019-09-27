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
import {Alert} from "reactstrap";
import {IAlertErrors} from "../../../constants";
import {getDisplayName} from "../../../utilities";

export interface IErrorProps {
    errors: IAlertErrors;
}

export function withErrors<T extends IErrorProps>(WrappedComponent: React.ComponentType<T>) {
    const errorsHoc = (props: T) => {
        const {errors} = props;

        return errors.hasErrors
            ? (
                <Alert color="danger">
                    {errors.general && <div>Unable to reach the api endpoint for this page.</div>}
                    {errors.specific.map((err, i) => <div key={i}>{err}</div>)}
                </Alert>
            )
            : <WrappedComponent {...props} />
    };
    (errorsHoc as React.FunctionComponent).displayName = `withErrors(${getDisplayName(WrappedComponent)})`;
    return errorsHoc;
}
