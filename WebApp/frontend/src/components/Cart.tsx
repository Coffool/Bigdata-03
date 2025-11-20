import { useState } from 'react';
import type { CartItem, CheckoutData, CustomerCreate } from '../types';
import { apiService } from '../api';
import { parsePrice, formatPrice } from '../utils';

interface CartProps {
  items: CartItem[];
  onUpdateQuantity: (trackId: number, quantity: number) => void;
  onRemoveItem: (trackId: number) => void;
  onClearCart: () => void;
}

export default function Cart({ items, onUpdateQuantity, onRemoveItem, onClearCart }: CartProps) {
  const [isCheckingOut, setIsCheckingOut] = useState(false);
  const [showCustomerForm, setShowCustomerForm] = useState(false);
  const [customerData, setCustomerData] = useState<CustomerCreate>({
    FirstName: '',
    LastName: '',
    Email: '',
  });
  const [billingInfo, setBillingInfo] = useState({
    BillingAddress: '',
    BillingCity: '',
    BillingCountry: '',
  });
  const [checkoutSuccess, setCheckoutSuccess] = useState<string>('');
  const [error, setError] = useState<string>('');

  const total = items.reduce((sum, item) => {
    return sum + (parsePrice(item.UnitPrice) * item.Quantity);
  }, 0);

  const handleQuantityChange = (trackId: number, newQuantity: number) => {
    if (newQuantity < 1) {
      onRemoveItem(trackId);
    } else {
      onUpdateQuantity(trackId, newQuantity);
    }
  };

  const handleCheckout = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (items.length === 0) {
      setError('El carrito está vacío');
      return;
    }

    setIsCheckingOut(true);
    setError('');

    try {
      // Primero crear el cliente
      const customerResponse = await apiService.createCustomer(customerData);
      
      // Preparar datos para checkout
      const checkoutData: CheckoutData = {
        CustomerId: customerResponse.CustomerId,
        BillingAddress: billingInfo.BillingAddress || undefined,
        BillingCity: billingInfo.BillingCity || undefined,
        BillingCountry: billingInfo.BillingCountry || undefined,
        Items: items.map(item => ({
          TrackId: item.TrackId,
          Quantity: item.Quantity,
          UnitPrice: item.UnitPrice,
        })),
      };

      // Procesar checkout
      const result = await apiService.checkout(checkoutData);
      
      setCheckoutSuccess(`¡Compra exitosa! Factura #${result.InvoiceId} - Total: $${result.Total}`);
      onClearCart();
      setShowCustomerForm(false);
      
      // Resetear formularios
      setCustomerData({ FirstName: '', LastName: '', Email: '' });
      setBillingInfo({ BillingAddress: '', BillingCity: '', BillingCountry: '' });
      
    } catch (err) {
      setError('Error al procesar la compra: ' + (err instanceof Error ? err.message : 'Error desconocido'));
    } finally {
      setIsCheckingOut(false);
    }
  };



  if (checkoutSuccess) {
    return (
      <div className="cart-container">
        <div className="success-message">
          <h2>✅ {checkoutSuccess}</h2>
          <button 
            onClick={() => setCheckoutSuccess('')}
            className="continue-shopping-btn"
          >
            Continuar comprando
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="cart-container">
      <h2>Carrito de Compras</h2>
      
      {error && <div className="error-message">{error}</div>}
      
      {items.length === 0 ? (
        <p className="empty-cart">Tu carrito está vacío</p>
      ) : (
        <>
          <div className="cart-items">
            {items.map((item) => (
              <div key={item.TrackId} className="cart-item">
                <div className="item-info">
                  <h4>{item.track?.Name}</h4>
                  <p className="item-composer">
                    {item.track?.Composer && `Compositor: ${item.track.Composer}`}
                  </p>
                </div>
                
                <div className="item-controls">
                  <div className="quantity-controls">
                    <button
                      onClick={() => handleQuantityChange(item.TrackId, item.Quantity - 1)}
                      className="quantity-btn"
                    >
                      -
                    </button>
                    <span className="quantity">{item.Quantity}</span>
                    <button
                      onClick={() => handleQuantityChange(item.TrackId, item.Quantity + 1)}
                      className="quantity-btn"
                    >
                      +
                    </button>
                  </div>
                  
                  <div className="item-price">
                    {formatPrice(item.UnitPrice)} x {item.Quantity} = {formatPrice(parsePrice(item.UnitPrice) * item.Quantity)}
                  </div>
                  
                  <button
                    onClick={() => onRemoveItem(item.TrackId)}
                    className="remove-btn"
                  >
                    Eliminar
                  </button>
                </div>
              </div>
            ))}
          </div>
          
          <div className="cart-summary">
            <div className="total">
              <strong>Total: {formatPrice(total)}</strong>
            </div>
            
            <div className="cart-actions">
              <button onClick={onClearCart} className="clear-cart-btn">
                Vaciar carrito
              </button>
              <button 
                onClick={() => setShowCustomerForm(true)} 
                className="checkout-btn"
              >
                Proceder al pago
              </button>
            </div>
          </div>

          {showCustomerForm && (
            <div className="checkout-form">
              <h3>Información del cliente</h3>
              <form onSubmit={handleCheckout}>
                <div className="form-section">
                  <h4>Datos personales</h4>
                  <div className="form-row">
                    <input
                      type="text"
                      placeholder="Nombre"
                      value={customerData.FirstName}
                      onChange={(e) => setCustomerData(prev => ({ ...prev, FirstName: e.target.value }))}
                      required
                      className="form-input"
                    />
                    <input
                      type="text"
                      placeholder="Apellido"
                      value={customerData.LastName}
                      onChange={(e) => setCustomerData(prev => ({ ...prev, LastName: e.target.value }))}
                      required
                      className="form-input"
                    />
                  </div>
                  <input
                    type="email"
                    placeholder="Email"
                    value={customerData.Email}
                    onChange={(e) => setCustomerData(prev => ({ ...prev, Email: e.target.value }))}
                    required
                    className="form-input full-width"
                  />
                </div>

                <div className="form-section">
                  <h4>Información de facturación (opcional)</h4>
                  <input
                    type="text"
                    placeholder="Dirección"
                    value={billingInfo.BillingAddress}
                    onChange={(e) => setBillingInfo(prev => ({ ...prev, BillingAddress: e.target.value }))}
                    className="form-input full-width"
                  />
                  <div className="form-row">
                    <input
                      type="text"
                      placeholder="Ciudad"
                      value={billingInfo.BillingCity}
                      onChange={(e) => setBillingInfo(prev => ({ ...prev, BillingCity: e.target.value }))}
                      className="form-input"
                    />
                    <input
                      type="text"
                      placeholder="País"
                      value={billingInfo.BillingCountry}
                      onChange={(e) => setBillingInfo(prev => ({ ...prev, BillingCountry: e.target.value }))}
                      className="form-input"
                    />
                  </div>
                </div>

                <div className="form-actions">
                  <button 
                    type="button" 
                    onClick={() => setShowCustomerForm(false)}
                    className="cancel-btn"
                  >
                    Cancelar
                  </button>
                  <button 
                    type="submit" 
                    disabled={isCheckingOut}
                    className="confirm-purchase-btn"
                  >
                    {isCheckingOut ? 'Procesando...' : `Confirmar compra - ${formatPrice(total)}`}
                  </button>
                </div>
              </form>
            </div>
          )}
        </>
      )}
    </div>
  );
}