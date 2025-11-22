# Despliegue de Chinook Music Store

Este directorio contiene todo lo necesario para desplegar la aplicación completa usando Docker.

## 🚀 Despliegue Rápido

### En tu máquina local:
```bash
docker-compose up --build
```

### En EC2:
```bash
./deploy.sh
```

## 📁 Estructura de archivos

- `docker-compose.yml` - Configuración de orquestación de servicios
- `backend/Dockerfile` - Imagen Docker para la API FastAPI
- `frontend/Dockerfile` - Imagen Docker para React con Nginx
- `deploy.sh` - Script automatizado para despliegue en EC2

## 🌐 Servicios

- **Frontend**: Puerto 80 (React + Nginx)
- **Backend**: Puerto 8000 (FastAPI)
- **Database**: Puerto 3306 (MySQL 8.0)

## 🔧 Configuración de EC2

### 1. Security Group
Configura el Security Group para permitir:
- Puerto 80 (HTTP) - Frontend
- Puerto 22 (SSH) - Acceso remoto
- Puerto 8000 (opcional) - API directa para desarrollo

### 2. Instalación
```bash
# Conectar a EC2
ssh -i tu-key.pem ubuntu@tu-ip-ec2

# Clonar repositorio
git clone <tu-repositorio>
cd Bigdata-03/WebApp

# Ejecutar script de despliegue
./deploy.sh
```

## 📋 Comandos útiles

```bash
# Ver logs de todos los servicios
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f backend
docker-compose logs -f frontend
docker-compose logs -f mysql

# Reiniciar servicios
docker-compose restart

# Detener todo
docker-compose down

# Limpiar todo (incluyendo volúmenes)
docker-compose down -v
docker system prune -a
```

## 🔍 Solución de problemas

### Base de datos no se conecta
```bash
# Verificar estado de MySQL
docker-compose exec mysql mysql -u app_user -p chinook -e "SHOW TABLES;"
```

### Frontend no carga
```bash
# Verificar configuración de Nginx
docker-compose exec frontend cat /etc/nginx/conf.d/default.conf
```

### Backend no responde
```bash
# Verificar logs del backend
docker-compose logs backend
```

## 🚨 Variables de entorno importantes

Las credenciales de la base de datos están definidas en `docker-compose.yml`:
- **Database**: chinook
- **User**: app_user
- **Password**: app_password
- **Root Password**: root_password

⚠️ **Para producción, cambia estas credenciales por variables de entorno seguras.**