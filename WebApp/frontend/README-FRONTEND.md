# Frontend de Chinook Music Store

## Descripción
Frontend desarrollado en React + TypeScript para interactuar con el backend de la tienda de música Chinook. La aplicación permite navegar por canciones, artistas, álbumes y realizar compras.

## Características

### 🎵 Navegación de Música
- **Canciones**: Lista paginada de canciones con búsqueda por nombre
- **Artistas**: Navegación por artistas
- **Álbumes**: Exploración de álbumes disponibles
- Información detallada incluyendo precio, duración y compositor

### 🛒 Carrito de Compras
- Agregar canciones al carrito
- Modificar cantidades
- Eliminar elementos
- Cálculo automático del total

### 💳 Proceso de Compra
- Formulario de información del cliente
- Datos de facturación opcionales
- Procesamiento de checkout integrado con el backend
- Confirmación de compra con número de factura

## Tecnologías Utilizadas
- React 19
- TypeScript
- Vite
- CSS3 con diseño responsivo

## Configuración

### Prerrequisitos
- Node.js 18+
- Backend ejecutándose en `http://localhost:8000`

### Instalación y Ejecución
```bash
# Instalar dependencias
npm install

# Ejecutar en modo desarrollo
npm run dev

# Compilar para producción
npm run build
```

## Estructura del Proyecto
```
src/
├── components/        # Componentes React
│   ├── Tracks.tsx    # Lista de canciones
│   ├── Artists.tsx   # Lista de artistas
│   ├── Albums.tsx    # Lista de álbumes
│   └── Cart.tsx      # Carrito de compras
├── types.ts          # Definiciones TypeScript
├── api.ts           # Servicio de API
├── App.tsx          # Componente principal
└── App.css          # Estilos globales
```

## API Integration
La aplicación se conecta a los siguientes endpoints del backend:

- `GET /tracks` - Lista de canciones con filtros
- `GET /artists` - Lista de artistas
- `GET /albums` - Lista de álbumes
- `POST /customers` - Crear cliente
- `POST /checkout` - Procesar compra
- `GET /health` - Estado del backend

## Características de UX/UI
- Diseño responsivo para móviles y desktop
- Navegación intuitiva por tabs
- Feedback visual para acciones (hover, loading)
- Manejo de errores con mensajes informativos
- Confirmaciones de compra exitosa
- Paginación con "cargar más"

## Próximas Mejoras Sugeridas
- Filtros avanzados (por género, precio, etc.)
- Integración con información de artistas y álbumes
- Historial de compras
- Wishlist o favoritos
- Autenticación de usuarios
- Modo oscuro
- Reproductor de música (preview)

## Notas de Desarrollo
- La aplicación asume que el backend está corriendo en localhost:8000
- Los precios se muestran en dólares americanos
- La validación se realiza tanto en frontend como backend
- Manejo de estados de carga y error en todas las operaciones