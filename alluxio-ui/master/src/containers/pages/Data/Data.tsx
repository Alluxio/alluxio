import React from 'react';
import {connect} from 'react-redux';
import {Alert, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {IFileInfo} from '@alluxio/common-ui/src/constants';
import {parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/data/actions';
import {IData} from '../../../store/data/types';

interface IPropsFromState {
  data: IData;
  errors: string;
  loading: boolean;
  location: {
    search: string;
  };
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IDataProps {
  refreshValue: boolean;
}

interface IDataState {
  limit?: string;
  offset?: string;
  lastFetched: {
    limit?: string;
    offset?: string;
  }
}

type AllProps = IPropsFromState & IPropsFromDispatch & IDataProps;

class Data extends React.Component<AllProps, IDataState> {
  constructor(props: AllProps) {
    super(props);

    const {offset, limit} = parseQuerystring(this.props.location.search);
    this.state = {offset, limit, lastFetched: {}};
  }

  public componentWillReceiveProps(props: AllProps) {
    const {refreshValue} = this.props;
    if (props.refreshValue !== refreshValue) {
      const {offset, limit} = this.state;
      this.fetchData(offset, limit);
    }
  }

  public componentDidUpdate(prevProps: AllProps) {
    if (this.props.location.search !== prevProps.location.search) {
      const {offset, limit} = parseQuerystring(this.props.location.search);
      this.setState({offset, limit});
      this.fetchData(offset, limit);
    }
  }

  public componentWillMount() {
    const {offset, limit} = this.state;
    this.fetchData(offset, limit);
  }

  public render() {
    const {data, errors} = this.props;

    if (errors || data.permissionError || data.fatalError) {
      return (
        <Alert color="danger">
          {errors && <div>Unable to reach the api endpoint for this page.</div>}
          {data.permissionError && <div>{data.permissionError}</div>}
          {data.fatalError && <div>{data.fatalError}</div>}
        </Alert>
      );
    }

    return (
      <div className="browse-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {this.renderFileListing(data.fileInfos)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileListing(fileInfos: IFileInfo[]) {
    return (
      <Table hover={true}>
        <thead>
        <tr>
          <th>File Path</th>
          <th>Size</th>
          <th>Block Size</th>
          <th>Permission</th>
          <th>Owner</th>
          <th>Group</th>
          <th>Pin</th>
          <th>Creation Time</th>
          <th>Modification Time</th>
        </tr>
        </thead>
        <tbody>
        {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
          <tr key={fileInfo.absolutePath}>
            <td>{fileInfo.absolutePath}</td>
            <td>{fileInfo.size}</td>
            <td>{fileInfo.blockSizeBytes}</td>
            <td>{fileInfo.mode}</td>
            <td>{fileInfo.owner}</td>
            <td>{fileInfo.group}</td>
            <td>{fileInfo.pinned}</td>
            <td>{fileInfo.creationTime}</td>
            <td>{fileInfo.modificationTime}</td>
          </tr>
        ))}
        </tbody>
      </Table>
    )
  }

  private fetchData(offset?: string, limit?: string) {
    this.setState({lastFetched: {offset, limit}});
    this.props.fetchRequest(offset, limit);
  }
}

const mapStateToProps = ({data}: IApplicationState) => ({
  data: data.data,
  errors: data.errors,
  loading: data.loading
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (offset?: string, limit?: string) => dispatch(fetchRequest(offset, limit))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Data);
