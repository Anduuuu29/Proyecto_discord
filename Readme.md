# Comandos para ejecutar el proyecto

## Ejecutar todos los servicios
```bash
# Iniciar Gateway
java Gateway

# Iniciar Gateway BACKUP
java Gateway backup

# Iniciar ServidorPrincipal
java ServidorPrincipal

# Iniciar ServidorPrincipal BACKUP
java ServidorPrincipal backup

# Iniciar ServidorChat
java ServidorChat

# Iniciar ServidorChat BACKUP
java ServidorChat backup

# Iniciar ServidorVoz
java ServidorVoz

# Iniciar ServidorVoz BACKUP
java ServidorVoz backup

# Iniciar Cliente
java Cliente

# Iniciar Cliente
java Cliente

```

## Ejecutar chat de voz
```bash
# Iniciar ServidorVoz (Si ya esta corriendo, se puede omitir este paso)
java ServidorVoz

# Iniciar ServidorVoz BACKUP (Si ya estan iniciados se puede omitir este paso)
java ServidorVoz backup

# Iniciar Cliente uno
java ClienteVoz

# Iniciar Cliente dos (Obligatoriamente deben ser minimo 2 clientes para probar el chat de voz)
java ClienteVoz
```

## Ejecutar tests de carga
```bash
# Ejecutar tests de carga
java -cp. LoadGenerator 50 60 30
```

