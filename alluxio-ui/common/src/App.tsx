import React from 'react';
import {LoadingMessage} from './components';

class App extends React.Component {
  public render() {
    return (
      <div className="App">
        <header className="App-header">
          <h1 className="App-title">Welcome to the Alluxio Shared Web Application</h1>
        </header>
        <p className="App-intro">
          This will contain reusable components for other web applications.
        </p>
        <p>
          Control list:
          <LoadingMessage/>
        </p>
      </div>
    );
  }
}

export default App;
