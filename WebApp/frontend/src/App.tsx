import { useState, useEffect } from 'react'
import type { CartItem } from './types'
import Tracks from './components/Tracks'
import Artists from './components/Artists'
import Albums from './components/Albums'
import Cart from './components/Cart'
import './App.css'

type View = 'tracks' | 'artists' | 'albums' | 'cart'

function App() {
  const [currentView, setCurrentView] = useState<View>('tracks')
  const [cart, setCart] = useState<CartItem[]>([])
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    // Simular una pequeña carga inicial para evitar flickering
    const timer = setTimeout(() => {
      setIsLoading(false)
    }, 100)
    
    return () => clearTimeout(timer)
  }, [])

  const addToCart = (newItem: CartItem) => {
    setCart(prevCart => {
      const existingItem = prevCart.find(item => item.TrackId === newItem.TrackId)
      
      if (existingItem) {
        return prevCart.map(item =>
          item.TrackId === newItem.TrackId
            ? { ...item, Quantity: item.Quantity + newItem.Quantity }
            : item
        )
      } else {
        return [...prevCart, newItem]
      }
    })
  }

  const updateQuantity = (trackId: number, quantity: number) => {
    setCart(prevCart =>
      prevCart.map(item =>
        item.TrackId === trackId ? { ...item, Quantity: quantity } : item
      )
    )
  }

  const removeFromCart = (trackId: number) => {
    setCart(prevCart => prevCart.filter(item => item.TrackId !== trackId))
  }

  const clearCart = () => {
    setCart([])
  }

  const getTotalItems = () => {
    return cart.reduce((total, item) => total + item.Quantity, 0)
  }

  const renderCurrentView = () => {
    switch (currentView) {
      case 'tracks':
        return <Tracks onAddToCart={addToCart} />
      case 'artists':
        return <Artists />
      case 'albums':
        return <Albums />
      case 'cart':
        return (
          <Cart
            items={cart}
            onUpdateQuantity={updateQuantity}
            onRemoveItem={removeFromCart}
            onClearCart={clearCart}
          />
        )
      default:
        return <Tracks onAddToCart={addToCart} />
    }
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
        <h1>🎵 Chinook Music Store</h1>
        
        <nav className="navigation">
          <button
            onClick={() => setCurrentView('tracks')}
            className={`nav-btn ${currentView === 'tracks' ? 'active' : ''}`}
          >
            Canciones
          </button>
          <button
            onClick={() => setCurrentView('artists')}
            className={`nav-btn ${currentView === 'artists' ? 'active' : ''}`}
          >
            Artistas
          </button>
          <button
            onClick={() => setCurrentView('albums')}
            className={`nav-btn ${currentView === 'albums' ? 'active' : ''}`}
          >
            Álbumes
          </button>
          <button
            onClick={() => setCurrentView('cart')}
            className={`nav-btn cart-btn ${currentView === 'cart' ? 'active' : ''}`}
          >
            🛒 Carrito ({getTotalItems()})
          </button>
        </nav>
      </header>

      <main className="main-content">
        {renderCurrentView()}
      </main>

      <footer className="app-footer">
        <p>© 2025 Chinook Music Store - Tienda de música online</p>
      </footer>
    </div>
  )
}

export default App
