import React from 'react';
import {connect} from 'react-redux';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {createDebouncedFunction, parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/logs/actions';
import {ILogs} from '../../../store/logs/types';

import './Logs.css';

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

type AllProps = IPropsFromState & IPropsFromDispatch;

class Logs extends React.Component<AllProps, ILogsState> {
  private readonly textAreaResizeMs = 100;
  private readonly debouncedUpdateTextAreaHeight = createDebouncedFunction(this.updateTextAreaHeight.bind(this), this.textAreaResizeMs, true);

  constructor(props: AllProps) {
    super(props);

    const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    this.state = {end, limit, offset, path, lastFetched: {}};
  }

  public componentDidUpdate(prevProps: AllProps) {
    if (this.props.location.search !== prevProps.location.search) {
      const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
      this.setState({path, offset, limit, end});
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
    const {loading} = this.props;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <div className="logs-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              Logs
            </div>
          </div>
        </div>
      </div>
    );
  }

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.setState({lastFetched: {path, offset, limit, end}});
    this.props.fetchRequest(path, offset, limit, end);
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
