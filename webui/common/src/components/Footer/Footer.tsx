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
import { Nav, NavItem, NavLink } from 'reactstrap';

import { INavigationData, INavigationDataCallbackParameters } from '../../constants';

import './Footer.css';

export interface IFooterProps {
  data: INavigationData[];
  callbackParameters?: INavigationDataCallbackParameters;
}

export class Footer extends React.PureComponent<IFooterProps> {
  constructor(props: IFooterProps) {
    super(props);

    this.renderNavItems = this.renderNavItems.bind(this);
  }

  public render(): JSX.Element {
    const { data } = this.props;
    return (
      <div className="footer mt-auto navbar-dark bg-primary">
        <Nav className="justify-content-center">{this.renderNavItems(data)}</Nav>
      </div>
    );
  }

  private renderNavItems(datas: INavigationData[]): JSX.Element[] {
    const { callbackParameters } = this.props;
    return datas.map((data: INavigationData) => {
      const url =
        typeof data.url === 'function' ? (callbackParameters ? data.url(callbackParameters) : data.url({})) : data.url;
      return (
        <NavItem key={url}>
          <NavLink href={url} {...data.attributes}>
            {data.innerText}
          </NavLink>
        </NavItem>
      );
    });
  }
}
