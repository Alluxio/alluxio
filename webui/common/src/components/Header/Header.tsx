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

import { faBell, faCheckSquare, faSquare } from '@fortawesome/free-regular-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { History } from 'history';
import React from 'react';
import { Link } from 'react-router-dom';
import {
  Button,
  ButtonGroup,
  Collapse,
  Nav,
  Navbar,
  NavbarBrand,
  NavbarToggler,
  NavItem,
  NavLink,
  Popover,
  PopoverBody,
} from 'reactstrap';

import { INavigationData, INavigationDataCallbackParameters } from '../../constants';
import logo from '../../images/alluxio-mark-tight-sm.svg';
import { isExternalLink } from '../../utilities';

import './Header.css';

export interface IHeaderProps {
  autoRefreshCallback?: (enable: boolean) => void;
  data: INavigationData[];
  callbackParameters?: INavigationDataCallbackParameters;
  history: History;
  newerVersionAvailable?: boolean;
}

interface IHeaderState {
  isAutoRefreshing: boolean;
  isOpen: boolean;
  isPopoverOpen: boolean;
  pathname: string;
}

export class Header extends React.PureComponent<IHeaderProps, IHeaderState> {
  constructor(props: IHeaderProps) {
    super(props);

    const {
      history: {
        location: { pathname },
      },
    } = this.props;
    this.toggleHamburgerMenu = this.toggleHamburgerMenu.bind(this);
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this);
    this.togglePopover = this.togglePopover.bind(this);
    this.renderNavItems = this.renderNavItems.bind(this);
    this.renderNewerVersionNotification = this.renderNewerVersionNotification.bind(this);
    this.closeHeaderOnClick = this.closeHeaderOnClick.bind(this);
    this.state = { isAutoRefreshing: false, isOpen: false, pathname, isPopoverOpen: false };
  }

  public componentDidMount(): void {
    const { history } = this.props;
    history.listen(
      (): void => {
        const {
          history: {
            location: { pathname },
          },
        } = this.props;
        this.setState(prevState => ({ ...prevState, ...{ pathname } }));
      },
    );
  }

  public render(): JSX.Element {
    const { isAutoRefreshing, isOpen } = this.state;
    const { data } = this.props;
    return (
      <div className={'header'}>
        <Navbar className="headerNavigation navbar-dark bg-primary pb-0" expand="lg">
          <NavbarBrand tag={Link} to="/" href="/">
            <div className="headerLogo align-top d-inline-block ml-lg-2">
              <div className="brand">
                <div className="brandImage d-inline-block">
                  <img className="brandSpin" src={logo} />
                </div>
                <div className="brandName d-inline-block align-middle pl-1">ALLUXIO</div>
              </div>
            </div>
          </NavbarBrand>
          <NavbarToggler className="mr-1" onClick={this.toggleHamburgerMenu} />
          <Collapse
            className={`mb-0 mt-2 d-lg-inline-flex justify-content-lg-center${isOpen ? '' : ' tabs'}`}
            isOpen={isOpen}
            navbar={true}
          >
            <Nav tabs={!isOpen} vertical={isOpen}>
              {this.renderNavItems(data)}
            </Nav>
          </Collapse>
          <Collapse
            className={`d-lg-inline-flex justify-content-lg-end${isOpen ? '' : ' tabs'}`}
            isOpen={isOpen}
            navbar={true}
          >
            <Nav tabs={!isOpen} vertical={isOpen}>
              <NavItem>
                {this.renderNewerVersionNotification()}
                <ButtonGroup className="auto-refresh-button mr-1">
                  <Button size="sm" color="secondary" onClick={this.toggleAutoRefresh} active={isAutoRefreshing}>
                    <FontAwesomeIcon icon={isAutoRefreshing ? faCheckSquare : faSquare} />
                    &nbsp; Auto Refresh
                  </Button>
                </ButtonGroup>
              </NavItem>
            </Nav>
          </Collapse>
        </Navbar>
      </div>
    );
  }

  private renderNavItems(datas: INavigationData[]): JSX.Element[] {
    const { pathname } = this.state;
    const { callbackParameters } = this.props;
    return datas.map((data: INavigationData) => {
      const url =
        typeof data.url === 'function' ? (callbackParameters ? data.url(callbackParameters) : data.url({})) : data.url;
      return (
        <NavItem key={url}>
          <NavLink
            className="text-white"
            tag={isExternalLink(url) ? NavLink : Link}
            to={url}
            href={url}
            active={pathname === url}
            onClick={this.closeHeaderOnClick}
            {...data.attributes}
          >
            {data.innerText}
          </NavLink>
        </NavItem>
      );
    });
  }

  private renderNewerVersionNotification(): JSX.Element | null {
    if (!this.props.newerVersionAvailable) return null;

    return (
      <ButtonGroup className="mr-1">
        <Button id="Popover1" size="sm" color="secondary">
          <FontAwesomeIcon icon={faBell} />
        </Button>
        <Popover
          placement="bottom"
          isOpen={this.state.isPopoverOpen}
          target="Popover1"
          toggle={this.togglePopover}
          trigger="hover"
        >
          <PopoverBody>
            <span>
              A new Alluxio version is available <a href="https://www.alluxio.io/download/">here</a>!
            </span>
          </PopoverBody>
        </Popover>
      </ButtonGroup>
    );
  }

  private closeHeaderOnClick(): void {
    this.setState({ isOpen: false });
  }

  private toggleAutoRefresh(): void {
    const { autoRefreshCallback } = this.props;
    let { isAutoRefreshing } = this.state;
    isAutoRefreshing = !isAutoRefreshing;
    this.setState({ isAutoRefreshing });
    if (autoRefreshCallback) {
      autoRefreshCallback(isAutoRefreshing);
    }
    this.closeHeaderOnClick();
  }

  private toggleHamburgerMenu(): void {
    this.setState({ isOpen: !this.state.isOpen });
  }

  private togglePopover(): void {
    this.setState({ isPopoverOpen: !this.state.isPopoverOpen });
  }
}
