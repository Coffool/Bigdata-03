import { useState, useEffect } from 'react';
import type { Album } from '../types';
import { apiService } from '../api';

export default function Albums() {
  const [albums, setAlbums] = useState<Album[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);

  const limit = 20;

  const loadAlbums = async (reset = false) => {
    try {
      setLoading(true);
      const currentOffset = reset ? 0 : offset;
      const newAlbums = await apiService.getAlbums(limit, currentOffset);
      
      if (reset) {
        setAlbums(newAlbums);
        setOffset(newAlbums.length);
      } else {
        setAlbums(prev => [...prev, ...newAlbums]);
        setOffset(prev => prev + newAlbums.length);
      }
      
      setHasMore(newAlbums.length === limit);
      setError('');
    } catch (err) {
      setError('Error al cargar los álbumes');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadAlbums(true);
  }, []);

  if (error) {
    return <div className="error">Error: {error}</div>;
  }

  return (
    <div className="albums-container">
      <h2>Álbumes</h2>
      
      {loading && albums.length === 0 ? (
        <div className="loading">Cargando álbumes...</div>
      ) : (
        <>
          <div className="albums-grid">
            {albums.map((album) => (
              <div key={album.AlbumId} className="album-card">
                <h3 className="album-title">{album.Title}</h3>
                <p className="album-artist">Artista ID: {album.ArtistId}</p>
                <p className="album-id">Álbum ID: {album.AlbumId}</p>
              </div>
            ))}
          </div>

          {hasMore && (
            <div className="load-more-container">
              <button
                onClick={() => loadAlbums()}
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