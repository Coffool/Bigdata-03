import type { Track, Album, Artist, Genre, Customer, CustomerCreate, CheckoutData, Invoice, CheckoutResponse, CustomerResponse } from './types';

const API_BASE_URL = 'http://localhost:8000'; // Ajusta la URL según tu configuración

class ApiService {
  private async request<T>(endpoint: string, options?: RequestInit): Promise<T> {
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
      headers: {
        'Content-Type': 'application/json',
        ...options?.headers,
      },
      ...options,
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return response.json();
  }

  // Track endpoints
  async getTracks(params: {
    limit?: number;
    offset?: number;
    name?: string;
    album_id?: number;
    genre_id?: number;
    min_price?: number;
    max_price?: number;
  } = {}): Promise<Track[]> {
    const searchParams = new URLSearchParams();
    
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        searchParams.append(key, value.toString());
      }
    });

    const queryString = searchParams.toString();
    const endpoint = `/tracks${queryString ? `?${queryString}` : ''}`;
    
    return this.request<Track[]>(endpoint);
  }

  async getTrack(trackId: number): Promise<Track> {
    return this.request<Track>(`/tracks/${trackId}`);
  }

  // Album endpoints
  async getAlbums(limit: number = 50, offset: number = 0): Promise<Album[]> {
    return this.request<Album[]>(`/albums?limit=${limit}&offset=${offset}`);
  }

  async getAlbum(albumId: number): Promise<Album> {
    return this.request<Album>(`/albums/${albumId}`);
  }

  // Artist endpoints
  async getArtists(limit: number = 50, offset: number = 0): Promise<Artist[]> {
    return this.request<Artist[]>(`/artists?limit=${limit}&offset=${offset}`);
  }

  async getArtist(artistId: number): Promise<Artist> {
    return this.request<Artist>(`/artists/${artistId}`);
  }

  // Genre endpoints
  async getGenres(): Promise<Genre[]> {
    return this.request<Genre[]>('/genres');
  }

  // Customer endpoints
  async createCustomer(customer: CustomerCreate): Promise<CustomerResponse> {
    return this.request<CustomerResponse>('/customers', {
      method: 'POST',
      body: JSON.stringify(customer),
    });
  }

  async getCustomer(customerId: number): Promise<Customer> {
    return this.request<Customer>(`/customers/${customerId}`);
  }

  // Checkout endpoint
  async checkout(data: CheckoutData): Promise<CheckoutResponse> {
    return this.request<CheckoutResponse>('/checkout', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  // Invoice endpoint
  async getInvoice(invoiceId: number): Promise<Invoice> {
    return this.request<Invoice>(`/invoices/${invoiceId}`);
  }

  // Health check
  async healthCheck(): Promise<{ status: string; service: string }> {
    return this.request<{ status: string; service: string }>('/health');
  }
}

export const apiService = new ApiService();