export interface Track {
  TrackId: number;
  Name: string;
  AlbumId?: number;
  MediaTypeId: number;
  GenreId?: number;
  Composer?: string;
  Milliseconds: number;
  Bytes?: number;
  UnitPrice: number | string; // Puede llegar como string desde el backend
}

export interface Album {
  AlbumId: number;
  Title: string;
  ArtistId: number;
}

export interface Artist {
  ArtistId: number;
  Name: string;
}

export interface Genre {
  GenreId: number;
  Name: string;
}

export interface Customer {
  CustomerId: number;
  FirstName: string;
  LastName: string;
  Email: string;
}

export interface CustomerCreate {
  FirstName: string;
  LastName: string;
  Email: string;
}

export interface CartItem {
  TrackId: number;
  Quantity: number;
  UnitPrice: number | string; // Puede llegar como string desde el backend
  track?: Track; // Para mostrar información adicional
}

export interface CheckoutData {
  CustomerId: number;
  BillingAddress?: string;
  BillingCity?: string;
  BillingCountry?: string;
  Items: CartItem[];
}

export interface Invoice {
  InvoiceId: number;
  CustomerId: number;
  InvoiceDate: string;
  BillingAddress?: string;
  BillingCity?: string;
  BillingCountry?: string;
  Total: number | string; // Puede llegar como string desde el backend
}

export interface CheckoutResponse {
  InvoiceId: number;
  Total: string;
  message: string;
}

export interface CustomerResponse {
  CustomerId: number;
  message: string;
}