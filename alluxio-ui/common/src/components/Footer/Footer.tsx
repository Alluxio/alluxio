import React from 'react';
import {Nav, NavItem, NavLink} from 'reactstrap';

import {INavigationData} from '../../constants';

import './Footer.css';

interface IFooterProps {
  data: INavigationData[];
}

export class Footer extends React.PureComponent<IFooterProps> {
  constructor(props: IFooterProps) {
    super(props);

    this.renderNavItems = this.renderNavItems.bind(this);
  }

  public render(): JSX.Element {
    const {data} = this.props;
    return (
      <div className="footer mt-auto card bg-light">
        <div className="mx-auto">
          <Nav>
            {this.renderNavItems(data)}
          </Nav>
        </div>
      </div>
    );
  }

  private renderNavItems(datas: INavigationData[]) {
    return datas.map((data: INavigationData) => (
      <NavItem key={data.url}>
        <NavLink href={data.url}>{data.innerText}</NavLink>
      </NavItem>
    ));
  }
}
