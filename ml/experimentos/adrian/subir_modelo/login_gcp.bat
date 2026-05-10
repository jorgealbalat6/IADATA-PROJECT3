@echo off
echo Iniciando proceso de autenticacion en Google Cloud...
:: Autenticacion general
call gcloud auth login
:: Autenticacion para las librerias de Python (ADC)
call gcloud auth application-default login
echo Autenticacion completada con exito.