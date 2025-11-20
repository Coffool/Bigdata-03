import React from 'react';

interface ErrorBoundaryState {
  hasError: boolean;
  error?: Error;
}

interface ErrorBoundaryProps {
  children: React.ReactNode;
}

class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('ErrorBoundary caught an error:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="error-boundary">
          <div className="error-container">
            <h2>🚨 Algo salió mal</h2>
            <p>Ha ocurrido un error inesperado en la aplicación.</p>
            <details>
              <summary>Detalles del error</summary>
              <pre>{this.state.error?.toString()}</pre>
            </details>
            <button 
              onClick={() => this.setState({ hasError: false, error: undefined })}
              className="retry-btn"
            >
              Intentar de nuevo
            </button>
            <button 
              onClick={() => window.location.reload()}
              className="reload-btn"
            >
              Recargar página
            </button>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;