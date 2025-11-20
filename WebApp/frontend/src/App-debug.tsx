import { useState, useEffect } from 'react'
import './App.css'

function App() {
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string>('')

  useEffect(() => {
    console.log('App component mounted')
    
    // Simular carga inicial
    const timer = setTimeout(() => {
      console.log('Loading finished')
      setIsLoading(false)
    }, 500)
    
    return () => {
      console.log('App component cleanup')
      clearTimeout(timer)
    }
  }, [])

  console.log('App render - isLoading:', isLoading, 'error:', error)

  if (error) {
    return (
      <div className="app">
        <div className="error-message">
          <h2>Error: {error}</h2>
          <button onClick={() => setError('')}>Reintentar</button>
        </div>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="app">
        <div className="loading-app">
          <h1>🎵 Cargando Chinook Music Store...</h1>
        </div>
      </div>
    )
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>🎵 Chinook Music Store - Versión Debug</h1>
        
        <nav className="navigation">
          <button className="nav-btn active">
            Debug Mode
          </button>
        </nav>
      </header>

      <main className="main-content">
        <div style={{ textAlign: 'center', padding: '2rem' }}>
          <h2>Aplicación cargada exitosamente</h2>
          <p>Si puedes ver este mensaje, el componente base funciona correctamente.</p>
          <p>El problema puede estar en uno de los componentes específicos.</p>
        </div>
      </main>

      <footer className="app-footer">
        <p>© 2025 Chinook Music Store - Debug Mode</p>
      </footer>
    </div>
  )
}

export default App