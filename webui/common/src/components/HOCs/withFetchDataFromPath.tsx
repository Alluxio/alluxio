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

import React, {ReactNode} from 'react';

import {parseQuerystring} from '../../utilities';
import {IRequest} from "../../constants";
import {getDisplayName} from "../../utilities/misc/getDisplayName";

interface IFetchDataPathState {
    end?: string;
    limit?: string;
    offset?: string;
    path?: string;
}

interface IFetchDataPathProps {
    location: {
        search: string;
    };
    refresh: boolean;
    fetchRequest: (req: IRequest) => void;
}

interface IHandlers {
    createInputChangeHandler: (stateKey: string, stateValueCallback: (value: string) => string | undefined)  => ((event: React.ChangeEvent<HTMLInputElement>) => void);
    createButtonHandler: (stateKey: string, stateValueCallback: (value?: string) => string | undefined) => ((event: React.MouseEvent<HTMLButtonElement>) => void);
}

export type IFetchDataPathType = IFetchDataPathState & IFetchDataPathProps & IHandlers & {queryStringSuffix: string};

export function withFetchDataFromPath<TWrappedComponentProps extends IFetchDataPathProps>(WrappedComponent: React.ComponentType<TWrappedComponentProps>) {
    class fetchDataFromPathHoc extends React.Component<TWrappedComponentProps, IFetchDataPathState> {
        constructor(props: TWrappedComponentProps) {
            super(props);

            let {path, offset, limit, end} = parseQuerystring(this.props.location.search);
            path = decodeURIComponent(path || '');
            offset = offset || '0';
            this.state = {end, limit, offset, path};

            this.createInputChangeHandler = this.createInputChangeHandler.bind(this);
            this.createButtonHandler = this.createButtonHandler.bind(this);
        }

        public componentWillMount() {
            const {path, offset, limit, end} = this.state;
            this.fetchData({path, offset, limit, end});
        }

        public componentDidUpdate(prevProps: TWrappedComponentProps) {
            const {refresh, location: {search}} = this.props;
            const {refresh: prevRefresh, location: {search: prevSearch}} = prevProps;
            if (search !== prevSearch) {
                let {path, offset, limit, end} = parseQuerystring(search);
                path = decodeURIComponent(path || '');
                offset = offset || '0';
                this.setState({path, offset, limit, end});
                this.fetchData({path, offset, limit, end});
            } else if (refresh !== prevRefresh) {
                const {path, offset, limit, end} = this.state;
                this.fetchData({path, offset, limit, end});
            }
        }

        public render(): (undefined | ReactNode) {
            let queryStringSuffix = Object.entries(this.state)
                .filter((obj: any[]) => ['offset', 'limit', 'end'].includes(obj[0]) && obj[1] != undefined)
                .map((obj: any) => `${obj[0]}=${obj[1]}`).join('&');
            queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;

            return <WrappedComponent {...this.props}
                                     {...this.state}
                                     createInputChangeHandler={this.createInputChangeHandler}
                                     createButtonHandler={this.createButtonHandler}
                                     queryStringSuffix={queryStringSuffix} />;
        }

        private fetchData(request: IRequest) {
            this.props.fetchRequest(request);
        }

        private createInputChangeHandler(stateKey: string, stateValueCallback: (value: string) => string | undefined) {
            return (event: React.ChangeEvent<HTMLInputElement>) => {
                const value = event.target.value;
                this.setState({...this.state, [stateKey]: stateValueCallback(value)});
            };
        }

        private createButtonHandler(stateKey: string, stateValueCallback: (value?: string) => string | undefined) {
            return (event: React.MouseEvent<HTMLButtonElement>) => {
                this.setState({...this.state, [stateKey]: stateValueCallback()});
            };
        }
    };
    (fetchDataFromPathHoc as React.ComponentType<any>).displayName = `withFetchDataFromPath(${getDisplayName(WrappedComponent)})`;
    return fetchDataFromPathHoc;
}
