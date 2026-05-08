# 📚 Guía Senior: Hiperparámetros de XGBoost

En el mundo del Machine Learning, XGBoost es como un coche de Fórmula 1: muy potente, pero con muchísimos botones en el volante. Si no los configuras bien, te estrellas (como hemos visto en el primer intento con ese AUC de 0.56).

Aquí tienes el "Diccionario Senior" de los parámetros que hemos usado para afinar el modelo y para qué sirve exactamente cada uno:

## ⚙️ Parámetros de Estructura del Árbol

*   **`n_estimators` (Número de árboles):**
    *   *Qué hace:* Define cuántos árboles de decisión va a construir el algoritmo en cadena (uno detrás de otro).
    *   *Uso Senior:* Poner muchos árboles (ej. `1000`) le da más tiempo al modelo para aprender detalles finos, pero aumenta el riesgo de *overfitting* (aprenderse los datos de memoria). Se controla combinándolo con `early_stopping_rounds`.
*   **`max_depth` (Profundidad máxima):**
    *   *Qué hace:* Cuántos niveles de ramas puede tener cada árbol. Un valor de `3` crea árboles enanos (poco detallistas), un `10` crea árboles inmensos.
    *   *Uso Senior:* A mayor profundidad, más interacciones complejas detecta (ej. "Lluvia + Fin de semana + Barrio céntrico"). Si es muy alto, memorizará el ruido (overfitting). Lo hemos subido a `8` porque nuestras relaciones de negocio parecen ser complejas.

## 🏃 Parámetros de Aprendizaje (Learning Rate)

*   **`learning_rate` (Tasa de aprendizaje - "eta"):**
    *   *Qué hace:* XGBoost funciona sumando las predicciones de muchos árboles. Este parámetro multiplica la contribución de cada nuevo árbol por un decimal (ej. `0.05`).
    *   *Uso Senior:* Si lo pones a `1.0`, el modelo avanza rapidísimo pero pega volantazos y se equivoca. Si lo pones a `0.01`, va muy lento y fino, pero necesitarás muchísimos `n_estimators` para que termine de aprender. `0.05` es un estándar de oro para encontrar el equilibrio.

## 🛡️ Parámetros de Regularización (Antídotos contra el Overfitting)

*   **`subsample` (Muestreo de filas):**
    *   *Qué hace:* Si vale `0.8`, cada vez que XGBoost va a construir un nuevo árbol, escoge al azar solo el 80% de las filas de tu dataset y oculta el 20% restante.
    *   *Uso Senior:* Es el mejor truco para forzar al modelo a generalizar. Como cada árbol ve datos ligeramente distintos, el conjunto final es mucho más robusto frente a años futuros (2026).
*   **`colsample_bytree` (Muestreo de columnas):**
    *   *Qué hace:* Igual que el anterior, pero en lugar de filas, escoge columnas al azar (ej. `0.8` escoge el 80% de las variables).
    *   *Uso Senior:* Impide que el modelo se "obsesione" solo con 1 o 2 variables obvias (como el `precio`). Le obliga a fijarse en variables secundarias (como la `lluvia`) para sacarles el jugo.

## ⚖️ Parámetros de Negocio y Objetivo

*   **`scale_pos_weight` (Balanceo de Clases):**
    *   *Qué hace:* Le dice al modelo cuánto "duele" fallar al predecir la clase minoritaria. 
    *   *Uso Senior:* En tu dataset, la ocupación (`1`) es minoritaria frente a los días libres (`0`). Si el modelo predice siempre `0`, acierta mucho estadísticamente, pero falla a nivel de negocio. Calculamos el ratio de desbalanceo (ej. `2.0`) y se lo pasamos. Así, fallar un `1` le resta el doble de puntos al modelo que fallar un `0`.
*   **`objective = 'binary:logistic'`:**
    *   *Qué hace:* Define la fórmula matemática interna. Le dice a XGBoost: *"Esto es una clasificación de 2 opciones, devuélveme un porcentaje (probabilidad)"*.
*   **`eval_metric = 'auc'`:**
    *   *Qué hace:* Le indica al modelo cómo medir su propio éxito durante el entrenamiento. El AUC penaliza muy bien al modelo cuando no sabe separar las dos clases, a diferencia del *Accuracy* tradicional que suele engañar en datasets desbalanceados.

## ⏱️ Parámetros de Control Operativo

*   **`early_stopping_rounds`:**
    *   *Qué hace:* Le dice al modelo: *"Si llevas 50 árboles seguidos creados y la métrica AUC del set de Test no ha mejorado absolutamente nada, detente y no construyas los 1000 árboles, porque estás perdiendo el tiempo"*.
*   **`n_jobs = -1`:**
    *   *Qué hace:* Usa todos los procesadores físicos de tu ordenador para paralelizar el trabajo matemático. Imprescindible para que el código corra rápido.
