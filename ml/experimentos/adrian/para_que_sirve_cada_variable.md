# 🧠 Diccionario de Variables (Features) y su Poder Predictivo

Nuestro objetivo (**el Target**) es predecir `is_occupied` (1 = ocupado, 0 = libre) para un piso concreto en un día concreto.

A continuación se dividen las *features* de la tabla `datos_modelo_ml_yannis` en 4 grandes bloques lógicos explicando su poder predictivo:

---

### 📅 1. Variables Temporales y Estacionales
Estas variables capturan el comportamiento cíclico del turismo. El turismo no es constante; se mueve por olas.

*   **`month` (Mes):** 
    *   *Qué es:* El mes del año (1 a 12).
    *   *Por qué es importante:* Capta la **estacionalidad macro**. Agosto no tiene la misma demanda que Noviembre. XGBoost usará esto para subir la probabilidad de ocupación en temporada alta de forma general.
*   **`day_of_week` (Día de la semana) & `is_weekend` (Es fin de semana):**
    *   *Qué son:* El día (1-Lunes a 7-Domingo) y un booleano (1 si es Sábado/Domingo).
    *   *Por qué son importantes:* Captan la **estacionalidad micro**. El turismo de escapada (city break) llena los pisos de jueves a domingo, dejando valles de lunes a miércoles.
*   **`date` (Fecha exacta):**
    *   *Nota:* Generalmente, la fecha exacta como "2025-06-23" no se le da directamente a XGBoost (porque no sabrá qué hacer con ella), pero se usa para separar los datos de entrenamiento (pasado) y validación/test (futuro).

---

### 🏠 2. Características del Piso (El Producto)
Estas variables le dicen al modelo cómo de "atractivo", accesible o grande es el piso comparado con su competencia.

*   **`neighbourhood_cleansed` (Barrio):**
    *   *Qué es:* El barrio donde está ubicado el piso.
    *   *Por qué es importante:* Es el factor espacial número uno. Un piso en el centro o cerca de la playa siempre tendrá una tasa de ocupación base mucho más alta que uno en la periferia.
*   **`listing_price` (Precio del piso ese día):**
    *   *Qué es:* El precio por noche.
    *   *Por qué es importante:* Ley de oferta y demanda. Precios artificialmente altos para lo que ofrece el piso bajarán drásticamente la probabilidad de reserva.
*   **`room_type` & `accommodates` (Tipo de cuarto y capacidad):**
    *   *Qué son:* Si es piso entero o habitación privada, y cuántas personas caben.
    *   *Por qué son importantes:* Definen al público objetivo. Un piso para 8 personas (`accommodates=8`) tendrá un patrón de reservas distinto (grupos/familias) que un estudio para 2 (parejas).
*   **`minimum_nights` (Noches mínimas):**
    *   *Por qué es importante:* Si un anfitrión exige 5 noches mínimas, se autoexcluye de todo el turismo de "escapada de fin de semana", bajando su ocupación media a cambio de estancias más largas y estables.
*   **`has_reviews`, `number_of_reviews`, `review_scores_rating` (Reputación):**
    *   *Qué son:* Si tiene reseñas, cuántas tiene y la nota media.
    *   *Por qué son importantes:* Generan **confianza**. En plataformas vacacionales, un piso sin reseñas (`has_reviews=0`) es muy difícil que se reserve frente a uno con 100 reseñas y nota de 4.8.
*   **`instant_bookable` (Reserva Inmediata):**
    *   *Qué es:* Si el huésped puede reservar sin esperar confirmación manual del anfitrión (True/False).
    *   *Por qué es importante:* A los usuarios no les gusta esperar. Los pisos con reserva inmediata suelen tener tasas de ocupación un 10-15% superiores.

---

### 🎆 3. Contexto Externo: Eventos y Festivos (Los Impulsores)
Estas variables explican las "anomalías" y picos de demanda que no se explican solo por ser agosto o fin de semana.

*   **`is_holiday` (Es festivo):**
    *   *Por qué es importante:* El turismo interno o local se mueve fundamentalmente en festivos y puentes. 
*   **`days_to_next_holiday` (Días hasta el próximo festivo):**
    *   *Qué es:* Cuenta atrás (ej. faltan 3 días para un puente).
    *   *Por qué es la variable "mágica":* Permite al XGBoost predecir **el comportamiento pre-festivo**. La gente suele reservar con mucha más intensidad los días inmediatamente anteriores a un puente largo.
*   **`num_sports`, `num_festivals`, `total_attendance` (Eventos):**
    *   *Qué son:* Cuántos eventos hay ese día y la suma total estimada de asistentes.
    *   *Por qué son importantes:* Si hay un gran partido (`num_sports`) o un macro-festival (`num_festivals`) que atrae a 100.000 personas (`total_attendance`), la demanda habitacional de la ciudad colapsa y la ocupación se dispara casi al 100%, ¡incluso con precios altos!

---

### 🌤️ 4. Contexto Externo: Meteorología
*   **`temp_mean` (Temperatura media) y `precipitation_mm` (Lluvia):**
    *   *Por qué son importantes:* En ciudades turísticas costeras, el turismo de "sol y playa" es vital. Aunque muchas reservas se hacen con meses de antelación, un mal pronóstico de lluvia (`precipitation_mm`) puede predecir aumentos en cancelaciones de última hora o caídas en las reservas cortas ("last minute"). 

---
**💡 Resumen del Modelo Mental de XGBoost:** 
El XGBoost combinará todo esto como si fuera un puzzle. Pensará algo como: *"Es Agosto (`month=8`), fin de semana (`is_weekend=1`), en un barrio top, con lluvia nula (`precipitation=0`) y hay un festival de 50k personas (`total_attendance=50000`). La probabilidad de ocupación es del 98%, a no ser que el precio (`listing_price`) sea desorbitado o el piso tenga una puntuación pésima (`review_scores_rating`)."*