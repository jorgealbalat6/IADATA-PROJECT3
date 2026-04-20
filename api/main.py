from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, field_validator
from google.cloud import firestore
import hashlib
import os

app = FastAPI(title="Usuarios API")

# Inicializa el cliente de Firestore
# - En local: usa GOOGLE_APPLICATION_CREDENTIALS apuntando al JSON del service account
# - En Cloud Run: usa el service account del servicio automáticamente (ADC)
db = firestore.Client()


# ──────────────────────────────────────────────
# Modelos
# ──────────────────────────────────────────────

class UsuarioInput(BaseModel):
    nombre: str
    email: EmailStr
    contrasena: str

    @field_validator("contrasena")
    @classmethod
    def contrasena_minima(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("La contraseña debe tener al menos 8 caracteres")
        return v


class UsuarioResponse(BaseModel):
    id: str
    nombre: str
    email: str
    mensaje: str


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def hashear_contrasena(contrasena: str) -> str:
    """Hash simple con SHA-256. En producción usa bcrypt o argon2."""
    return hashlib.sha256(contrasena.encode()).hexdigest()


# ──────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────

@app.post("/usuarios", response_model=UsuarioResponse, status_code=201)
async def crear_usuario(usuario: UsuarioInput):
    """Crea un nuevo usuario en la colección 'usuarios' de Firestore."""

    # Verificar si el email ya existe
    usuarios_ref = db.collection("usuarios")
    query = usuarios_ref.where("email", "==", usuario.email).limit(1).get()

    if len(query) > 0:
        raise HTTPException(
            status_code=409,
            detail=f"Ya existe un usuario con el email {usuario.email}"
        )

    # Preparar documento
    nuevo_usuario = {
        "nombre": usuario.nombre,
        "email": usuario.email,
        "contrasena": hashear_contrasena(usuario.contrasena),
    }

    # Insertar en Firestore (ID generado automáticamente)
    doc_ref = usuarios_ref.add(nuevo_usuario)
    doc_id = doc_ref[1].id

    return UsuarioResponse(
        id=doc_id,
        nombre=usuario.nombre,
        email=usuario.email,
        mensaje="Usuario creado exitosamente",
    )


@app.get("/health")
async def health():
    return {"status": "ok"}
