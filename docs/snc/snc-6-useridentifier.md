# Salida No Conforme — SNC-6

---

**1. Fecha de detección**
2026-05-20

---

**2. Persona que registra**
Stefany López

---

**3. Proceso afectado**
DESARROLLO DE SOFTWARE, AUTOMATIZACIÓN E IA

---

**4. Descripción del producto o salida no conforme**

Durante la subida de campañas de email en operación se detectó que el identificador de usuario no estaba siendo guardado en la base de datos. Al revisar, se identificaron dos omisiones en la funcionalidad `userIdentifier` que no habían sido contempladas:

- El sistema no verificaba que la columna identificadora estuviera correctamente configurada antes de procesar, permitiendo continuar con una configuración incompleta sin notificar al usuario.
- El identificador del usuario no estaba siendo guardado en los resultados del procesamiento, por lo que el dato no llegaba a la base de datos.

---

**5. Tratamiento de la SNC**

- [x] Corrección

---

**6. Describa el análisis de causas y causa raíz**

La funcionalidad `userIdentifier` fue desarrollada sin contemplar el flujo completo de extremo a extremo. No se definió la dependencia de validación entre los campos `userIdentifier` y `nameColumnIdentifier`, ni se estableció como requisito la persistencia del identificador en el Parquet de salida. La omisión no fue detectada durante el desarrollo porque no se realizaron pruebas que cubrieran el flujo completo desde la captura del identificador hasta su almacenamiento en base de datos.

---

**7. Describa las acciones realizadas**

- Se implementó la validación de la configuración del identificador antes del procesamiento.
- Se corrigió el guardado del identificador en los resultados para que el dato llegue correctamente a la base de datos.
- Se verificó el comportamiento correcto mediante pruebas en los servicios afectados.

---

**8. Responsables del cierre de la SNC**
Stefany López

---

**9. Fecha de cierre**
2026-05-20

---

**10. Verificación de eficacia**

- [x] SÍ
- [ ] NO
