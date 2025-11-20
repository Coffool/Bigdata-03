import { useState, useEffect, useCallback } from 'react';
import type { Track, CartItem } from '../types';
import { apiService } from '../api';
import { formatPrice, formatDuration } from '../utils';

interface TracksProps {
  onAddToCart: (item: CartItem) => void;
}

export default function Tracks({ onAddToCart }: TracksProps) {
  const [tracks, setTracks] = useState<Track[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [searchName, setSearchName] = useState('');
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);

  const limit = 20;

  const loadTracks = useCallback(async (reset = false, customSearchName?: string) => {
    try {
      setLoading(true);
      const currentOffset = reset ? 0 : offset;
      const searchTerm = customSearchName !== undefined ? customSearchName : searchName;
      
      const newTracks = await apiService.getTracks({
        limit,
        offset: currentOffset,
        name: searchTerm || undefined,
      });
      
      if (reset) {
        setTracks(newTracks);
        setOffset(newTracks.length);
      } else {
        setTracks(prev => [...prev, ...newTracks]);
        setOffset(prev => prev + newTracks.length);
      }
      
      setHasMore(newTracks.length === limit);
      setError('');
    } catch (err) {
      setError('Error al cargar las canciones');
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, []); // Sin dependencias para evitar loops

  // Effect inicial - solo se ejecuta una vez
  useEffect(() => {
    loadTracks(true);
  }, []); // Dependencias vacías

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setOffset(0);
    loadTracks(true, searchName);
  };

  const handleAddToCart = (track: Track) => {
    onAddToCart({
      TrackId: track.TrackId,
      Quantity: 1,
      UnitPrice: track.UnitPrice,
      track,
    });
  };

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  return (
    <div className="tracks-container">
      <h2>Canciones</h2>
      
      <form onSubmit={handleSearch} className="search-form">
        <input
          type="text"
          value={searchName}
          onChange={(e) => setSearchName(e.target.value)}
          placeholder="Buscar canciones..."
          className="search-input"
        />
        <button type="submit" className="search-btn">Buscar</button>
      </form>

      {loading && tracks.length === 0 ? (
        <div className="loading">Cargando canciones...</div>
      ) : (
        <>
          <div className="tracks-grid">
            {tracks.map((track) => (
              <div key={track.TrackId} className="track-card">
                <div className="track-info">
                  <h3 className="track-name">{track.Name}</h3>
                  {track.Composer && (
                    <p className="track-composer">Compositor: {track.Composer}</p>
                  )}
                  <p className="track-duration">
                    Duración: {formatDuration(track.Milliseconds)}
                  </p>
                  <p className="track-price">{formatPrice(track.UnitPrice)}</p>
                </div>
                <button
                  onClick={() => handleAddToCart(track)}
                  className="add-to-cart-btn"
                >
                  Agregar al carrito
                </button>
              </div>
            ))}
          </div>

          {hasMore && (
            <div className="load-more-container">
              <button
                onClick={() => loadTracks(false, searchName)}
                disabled={loading}
                className="load-more-btn"
              >
                {loading ? 'Cargando...' : 'Cargar más'}
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}