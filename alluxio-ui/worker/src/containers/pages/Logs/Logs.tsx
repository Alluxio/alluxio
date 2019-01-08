import React from 'react';
import {connect} from 'react-redux';
import {Link} from 'react-router-dom';
import {Alert, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {FileView} from '@alluxio/common-ui/src/components';
import {IFileInfo} from '@alluxio/common-ui/src/constants';
import {getDebouncedFunction, parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/logs/actions';
import {ILogs} from '../../../store/logs/types';

interface IPropsFromState {
  errors: string;
  loading: boolean;
  location: {
    search: string;
  };
  logs: ILogs;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface ILogsProps {
  refreshValue: boolean;
}

interface ILogsState {
  end?: string;
  limit?: string;
  offset?: string;
  path?: string;
  lastFetched: {
    end?: string;
    limit?: string;
    offset?: string;
    path?: string;
  };
  textAreaHeight?: number;
}

type AllProps = IPropsFromState & IPropsFromDispatch & ILogsProps;

class Logs extends React.Component<AllProps, ILogsState> {
  private readonly textAreaResizeMs = 100;
  private readonly debouncedUpdateTextAreaHeight = getDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

  constructor(props: AllProps) {
    super(props);

    const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    this.state = {end, limit, offset, path, lastFetched: {}};
  }

  public componentDidUpdate(prevProps: AllProps) {
    const {refreshValue, location: {search}} = this.props;
    const {refreshValue: prevRefreshValue, location: {search: prevSearch}} = prevProps;
    if (search !== prevSearch) {
      const {path, offset, limit, end} = parseQuerystring(search);
      this.setState({path, offset, limit, end});
      this.fetchData(path, offset, limit, end);
    }
    if (refreshValue !== prevRefreshValue) {
      const {path, offset, limit, end} = this.state;
      this.fetchData(path, offset, limit, end);
    }
  }

  public componentWillMount() {
    const {path, offset, limit, end} = this.state;
    this.fetchData(path, offset, limit, end);
    this.updateTextAreaHeight();
  }

  public componentDidMount() {
    window.addEventListener('resize', this.debouncedUpdateTextAreaHeight);
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.debouncedUpdateTextAreaHeight);
  }

  public render() {
    const {errors, logs} = this.props;
    let queryStringSuffix = ['offset', 'limit', 'end'].filter((key: string) => this.state[key] !== undefined)
      .map((key: string) => `${key}=${this.state[key]}`).join('&');
    queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;

    if (errors || logs.invalidPathError || logs.fatalError) {
      return (
        <Alert color="danger">
          {errors && <div>Unable to reach the api endpoint for this page.</div>}
          {logs.invalidPathError && <div>{logs.invalidPathError}</div>}
          {logs.fatalError && <div>{logs.fatalError}</div>}
        </Alert>
      );
    }

    return (
      <div className="logs-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {logs.fileData && this.renderFileView(logs, queryStringSuffix)}
              {!logs.fileData && this.renderDirectoryListing(logs.fileInfos, queryStringSuffix)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderFileView(logs: ILogs, queryStringSuffix: string) {
    const {textAreaHeight, path, offset, end, lastFetched} = this.state;
    const offsetInputHandler = this.createInputHandler('offset', value => value).bind(this);
    const beginInputHandler = this.createInputHandler('end', value => undefined).bind(this);
    const endInputHandler = this.createInputHandler('end', value => '1').bind(this);
    return (
        <FileView beginInputHandler={beginInputHandler} end={end} endInputHandler={endInputHandler}
                  lastFetched={lastFetched} offset={offset} offsetInputHandler={offsetInputHandler} path={path}
                  queryStringPrefix="/logs" queryStringSuffix={queryStringSuffix} textAreaHeight={textAreaHeight}
                  viewData={logs}/>
    );
  }

  private renderDirectoryListing(fileInfos: IFileInfo[], queryStringSuffix: string) {
    return (
      <Table hover={true}>
        <thead>
        <tr>
          <th>File Name</th>
          <th>Size</th>
          <th>Block Size</th>
          <th>In-Alluxio</th>
          <th>Persistence State</th>
          <th>Pin</th>
          <th>Creation Time</th>
          <th>Modification Time</th>
          <th>[D]DepID</th>
          <th>[D]INumber</th>
          <th>[D]UnderfsPath</th>
          <th>[D]File Locations</th>
        </tr>
        </thead>
        <tbody>
        {fileInfos && fileInfos.map((fileInfo: IFileInfo) => (
          <tr key={fileInfo.absolutePath}>
            <td>
              {this.renderFileNameLink(fileInfo.absolutePath, queryStringSuffix)}
            </td>
            <td>{fileInfo.size}</td>
            <td>{fileInfo.blockSizeBytes}</td>
            <td>{fileInfo.inAlluxioPercentage}%</td>
            <td>{fileInfo.persistenceState}</td>
            <td>{fileInfo.pinned ? 'YES' : 'NO'}</td>
            <td>{fileInfo.creationTime}</td>
            <td>{fileInfo.modificationTime}</td>
            <td>{fileInfo.id}</td>
            <td>
              {/*TODO: what goes here?*/}
            </td>
            <td>{fileInfo.absolutePath}</td>
            <td>
              {fileInfo.fileLocations.map((location: string) => (
                <div key={location}>{location}</div>
              ))}
            </td>
          </tr>
        ))}
        </tbody>
      </Table>
    )
  }

  private renderFileNameLink(filePath: string, queryStringSuffix: string) {
    const {lastFetched} = this.state;
    if (filePath === lastFetched.path) {
      return (
        filePath
      );
    }

    return (
      <Link to={`/logs?path=${filePath}${queryStringSuffix}`}>
        {filePath}
      </Link>
    );
  }

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.setState({lastFetched: {path, offset, limit, end}});
    this.props.fetchRequest(path, offset, limit, end);
  }

  private createInputHandler(stateKey: string, stateValueCallback: (value: string) => string | undefined) {
    return (event: React.ChangeEvent<HTMLInputElement>) => {
      const value = event.target.value;
      const state = {};
      state[stateKey] = stateValueCallback(value);
      this.setState(state);
    };
  }

  private updateTextAreaHeight() {
    this.setState({textAreaHeight: window.innerHeight / 2});
  }
}

const mapStateToProps = ({logs}: IApplicationState) => ({
  errors: logs.errors,
  loading: logs.loading,
  logs: logs.logs
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Logs);
