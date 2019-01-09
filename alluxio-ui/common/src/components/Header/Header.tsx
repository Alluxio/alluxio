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

import {faCheckSquare, faSquare} from '@fortawesome/free-regular-svg-icons'
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome'
import {Action, Location} from 'history';
import React from 'react';
import {Link} from 'react-router-dom';
import {
  Button,
  ButtonGroup,
  Collapse,
  Nav,
  Navbar,
  NavbarBrand,
  NavbarToggler,
  NavItem,
  NavLink
} from 'reactstrap';

import {INavigationData} from '../../constants';
import logo from '../../images/alluxio-mark-tight-sm.svg';
import {isExternalLink} from '../../utilities';

import './Header.css';

interface IHeaderProps {
  history: any;
  data: INavigationData[];
  autoRefreshCallback?: (enable: boolean) => void;
}

interface IHeaderState {
  isAutoRefreshing: boolean;
  isOpen: boolean;
  pathname: string;
}

export class Header extends React.PureComponent<IHeaderProps, IHeaderState> {
  constructor(props: IHeaderProps) {
    super(props);

    const {history: {location: {pathname}}} = this.props;
    this.toggleHamburgerMenu = this.toggleHamburgerMenu.bind(this);
    this.toggleAutoRefresh = this.toggleAutoRefresh.bind(this);
    this.renderNavItems = this.renderNavItems.bind(this);
    this.closeHeaderOnClick = this.closeHeaderOnClick.bind(this);
    this.state = {isAutoRefreshing: false, isOpen: false, pathname};
  }

  public componentDidMount() {
    const {history} = this.props;
    history.listen((loc: Location, action: Action) => {
      const {history: {location: {pathname}}} = this.props;
      this.setState((prevState) => (
        {...prevState, ...{pathname}})
      );
    });
  }

  public render(): JSX.Element {
    const {isAutoRefreshing, isOpen} = this.state;
    const {data} = this.props;
    return (
      <div className={'header card bg-light'}>
        <Navbar className="headerNavigation" expand="lg" light={true}>
          <NavbarBrand tag={Link} to="/" href="/">
            <div className="headerLogo align-top d-inline-block ml-lg-2">
              <div className="brand">
                <div className="brandImage d-inline-block">
                  <img className="brandSpin" src={logo}/>
                </div>
                <div className="brandName d-inline-block align-middle pl-1">
                  ALLUXIO
                </div>
              </div>
            </div>
          </NavbarBrand>
          <NavbarToggler className="mr-1" onClick={this.toggleHamburgerMenu}/>
          <Collapse className={`d-lg-inline-flex justify-content-lg-center${isOpen ? '' : ' tabs'}`}
                    isOpen={isOpen} navbar={true}>
            <Nav tabs={!isOpen} vertical={isOpen}>
              {this.renderNavItems(data)}
            </Nav>
          </Collapse>
          <Collapse className={`d-lg-inline-flex justify-content-lg-end${isOpen ? '' : ' tabs'}`}
                    isOpen={isOpen} navbar={true}>
            <Nav tabs={!isOpen} vertical={isOpen}>
              <NavItem>
                <ButtonGroup className="auto-refresh-button">
                  <Button size="sm" outline={true}
                          color={isAutoRefreshing ? 'primary' : 'secondary'}
                          onClick={this.toggleAutoRefresh} active={isAutoRefreshing}>
                    <FontAwesomeIcon icon={isAutoRefreshing ? faCheckSquare : faSquare}/>&nbsp;
                    Auto Refresh
                  </Button>
                </ButtonGroup>
              </NavItem>
            </Nav>
          </Collapse>
        </Navbar>
      </div>
    );
  }

  private renderNavItems(datas: INavigationData[]) {
    const {pathname} = this.state;
    return datas.map((data: INavigationData) => {
      const url = typeof data.url === 'function' ? data.url() : data.url;
      return (
      <NavItem key={url}>
        <NavLink tag={isExternalLink(url) ? NavLink : Link} to={url} href={url}
                 active={pathname === url} onClick={this.closeHeaderOnClick}>{data.innerText}</NavLink>
      </NavItem>
    )});
  }

  private closeHeaderOnClick() {
    this.setState({isOpen: false});
  }

  private toggleAutoRefresh() {
    const {autoRefreshCallback} = this.props;
    let {isAutoRefreshing} = this.state;
    isAutoRefreshing = !isAutoRefreshing;
    this.setState({isAutoRefreshing});
    if (autoRefreshCallback) {
      autoRefreshCallback(isAutoRefreshing);
    }
    this.closeHeaderOnClick();
  }

  private toggleHamburgerMenu() {
    this.setState({isOpen: !this.state.isOpen});
  }
}
