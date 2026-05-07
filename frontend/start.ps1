# Script de inicio del frontend - Pricing Frontend
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "   Iniciando Pricing Frontend (Vite)    " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Verificar que Node.js está instalado
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Host "[ERROR] Node.js no está instalado. Por favor instálalo desde https://nodejs.org" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Node.js version: $(node -v)" -ForegroundColor Green
Write-Host "[OK] npm version: $(npm -v)" -ForegroundColor Green

# Instalar dependencias si node_modules no existe
if (-not (Test-Path "node_modules")) {
    Write-Host "`n[INFO] Instalando dependencias..." -ForegroundColor Yellow
    npm install
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Falló la instalación de dependencias." -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Dependencias instaladas correctamente." -ForegroundColor Green
} else {
    Write-Host "[OK] Dependencias ya instaladas." -ForegroundColor Green
}

# Iniciar el servidor de desarrollo
Write-Host "`n[INFO] Arrancando servidor de desarrollo en http://localhost:5173 ..." -ForegroundColor Yellow
npm run dev
