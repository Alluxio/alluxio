import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/blockInfo/actions';
import {IBlockInfo} from '../../../store/blockInfo/types';

import './BlockInfo.css';

interface IPropsFromState {
  blockInfo: IBlockInfo;
  errors: string;
  loading: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class BlockInfo extends React.Component<AllProps> {
  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {loading} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <div className="blockInfo-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Storage Usage Summary</h5>
              BlockInfo
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const mapStateToProps = ({blockInfo}: IApplicationState) => ({
  blockInfo: blockInfo.blockInfo,
  errors: blockInfo.errors,
  loading: blockInfo.loading
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: () => dispatch(fetchRequest())
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(BlockInfo);
