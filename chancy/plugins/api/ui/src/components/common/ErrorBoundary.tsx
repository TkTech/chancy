import React from 'react';

type Props = { children: React.ReactNode };
type State = { error: Error | null };

export class ErrorBoundary extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    // eslint-disable-next-line no-console
    console.error('UI ErrorBoundary caught an error', error, errorInfo);
  }

  render() {
    if (this.state.error) {
      return (
        <div className="container py-4">
          <div className="alert alert-danger">
            <h4 className="alert-heading">Something went wrong.</h4>
            <p>{this.state.error.message}</p>
            <hr />
            <button className="btn btn-sm btn-primary" onClick={() => window.location.reload()}>Reload</button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

