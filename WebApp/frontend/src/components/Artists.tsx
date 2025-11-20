import { useState, useEffect } from 'react';
import type { Artist } from '../types';
import { apiService } from '../api';

export default function Artists() {
  const [artists, setArtists] = useState<Artist[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);

  const limit = 20;

  const loadArtists = async (reset = false) => {
    try {
      setLoading(true);
      const currentOffset = reset ? 0 : offset;
      const newArtists = await apiService.getArtists(limit, currentOffset);
      
      if (reset) {
        setArtists(newArtists);
        setOffset(newArtists.length);
      } else {
        setArtists(prev => [...prev, ...newArtists]);
        setOffset(prev => prev + newArtists.length);
      }
      
      setHasMore(newArtists.length === limit);
      setError('');
    } catch (err) {
      setError('Error al cargar los artistas');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadArtists(true);
  }, []);

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  return (
    <div className="artists-container">
      <h2>Artistas</h2>
      
      {loading && artists.length === 0 ? (
        <div className="loading">Cargando artistas...</div>
      ) : (
        <>
          <div className="artists-grid">
            {artists.map((artist) => (
              <div key={artist.ArtistId} className="artist-card">
                <h3 className="artist-name">{artist.Name}</h3>
                <p className="artist-id">ID: {artist.ArtistId}</p>
              </div>
            ))}
          </div>

          {hasMore && (
            <div className="load-more-container">
              <button
                onClick={() => loadArtists()}
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