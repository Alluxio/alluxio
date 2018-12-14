import React from 'react';

import logo from '../../images/alluxio-mark-tight-sm.svg';

import './LoadingMessage.css';

export class LoadingMessage extends React.PureComponent {
  public render() {
    return (
      <div className="loadingMessage text-center">
        <img className="loadingMessageSpin" src={logo}/>
        <br/>
        <h5 className="">Please wait...</h5>
      </div>
    );
  }
}

export default LoadingMessage;
