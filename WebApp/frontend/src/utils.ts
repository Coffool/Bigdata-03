// Utilidades para manejar tipos de datos del backend

/**
 * Convierte un precio que puede venir como string o number del backend a un número válido
 */
export const parsePrice = (price: number | string | null | undefined): number => {
  if (price === null || price === undefined) {
    return 0;
  }
  
  if (typeof price === 'number') {
    return isNaN(price) ? 0 : price;
  }
  
  if (typeof price === 'string') {
    const parsed = parseFloat(price);
    return isNaN(parsed) ? 0 : parsed;
  }
  
  return 0;
};

/**
 * Formatea un precio para mostrar con el símbolo de dólar
 */
export const formatPrice = (price: number | string | null | undefined): string => {
  const numericPrice = parsePrice(price);
  return `$${numericPrice.toFixed(2)}`;
};

/**
 * Convierte milisegundos a formato MM:SS
 */
export const formatDuration = (milliseconds: number): string => {
  if (!milliseconds || milliseconds < 0) {
    return '0:00';
  }
  
  const minutes = Math.floor(milliseconds / 60000);
  const seconds = Math.floor((milliseconds % 60000) / 1000);
  return `${minutes}:${seconds.toString().padStart(2, '0')}`;
};