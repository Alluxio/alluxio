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
import { IRequest } from '../../../constants';
import { getDisplayName, parseQueryString } from '../../../utilities';

interface IFetchDataFromPathState {
  request: IRequest;
}

export interface IFetchDataFromPathProps {
  fetchRequest: (req: IRequest) => void;
  location: {
    search: string;
  };
  refresh: boolean;
}

export function withFetchDataFromPath<T extends IFetchDataFromPathProps>(
  WrappedComponent: React.ComponentType<T>,
): React.ComponentType<T> {
  class FetchDataFromPathHoc extends React.Component<T, IFetchDataFromPathState> {
    constructor(props: T) {
      super(props);

      this.state = { request: this.getParsedQuery(this.props.location.search) };

      this.updateRequestParameter = this.updateRequestParameter.bind(this);
    }

    public componentWillMount(): void {
      this.fetchData(this.state.request);
    }

    public componentDidUpdate(prevProps: T): void {
      const {
        refresh,
        location: { search },
      } = this.props;
      const {
        refresh: prevRefresh,
        location: { search: prevSearch },
      } = prevProps;
      if (search !== prevSearch) {
        const request = this.getParsedQuery(search);
        this.setState({ request: request });
        this.fetchData(request);
      } else if (refresh !== prevRefresh) {
        this.fetchData(this.state.request);
      }
    }

    public render(): JSX.Element {
      return (
        <WrappedComponent
          {...this.props}
          {...this.state}
          updateRequestParameter={this.updateRequestParameter}
          queryStringSuffix={this.getQueryStringSuffix()}
        />
      );
    }

    private fetchData(request: IRequest): void {
      this.props.fetchRequest(request);
    }

    private updateRequestParameter(stateKey: string, value: string | undefined): void {
      const updatedReq: IRequest = { ...this.state.request, [stateKey]: value };
      this.setState({ request: updatedReq });
    }

    private getQueryStringSuffix(): string {
      let queryStringSuffix = Object.entries(this.state.request)
        .filter((obj: string[]) => ['offset', 'limit', 'end'].includes(obj[0]) && obj[1] != undefined)
        .map((obj: string[]) => `${obj[0]}=${obj[1]}`)
        .join('&');
      queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;
      return queryStringSuffix;
    }

    private getParsedQuery(query: string): IRequest {
      const parsed = parseQueryString(query);
      let { path, offset } = parsed;
      const { limit, end } = parsed;
      path = decodeURIComponent(path || '');
      offset = offset || '0';
      return { path, offset, limit, end };
    }
  }
  (FetchDataFromPathHoc as React.ComponentType<T>).displayName = `withFetchDataFromPath(${getDisplayName(
    WrappedComponent,
  )})`;
  return FetchDataFromPathHoc;
}
