import React from 'react';
import {connect} from 'react-redux';
import {Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {IConfigTriple} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/config/actions';
import {IConfig} from '../../../store/config/types';

import './Configuration.css';

interface IPropsFromState {
  errors: string;
  loading: boolean;
  config: IConfig;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Configuration extends React.Component<AllProps> {
  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {loading, config} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <div className="configuration-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Alluxio Configuration</h5>
              <Table hover={true}>
                <thead>
                <tr>
                  <th>Property</th>
                  <th>Value</th>
                  <th>Source</th>
                </tr>
                </thead>
                <tbody>
                {config.configuration.map((configuration: IConfigTriple) => (
                  <tr key={configuration.left}>
                    <td>{configuration.left}</td>
                    <td>{configuration.middle}</td>
                    <td>{configuration.right}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
            <div className="col-12">
              <h5>Whitelist</h5>
              <Table hover={true}>
                <tbody>
                {config.whitelist.map((whitelist: string) => (
                  <tr key={whitelist}>
                    <td>{whitelist}</td>
                  </tr>
                ))}
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({config}: IApplicationState) => ({
  config: config.config,
  errors: config.errors,
  loading: config.loading
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Configuration);
