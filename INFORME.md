# Informe de Coordinación y Escalabilidad

## Identificación de clientes

Para separar los flujos de datos de distintos clientes concurrentes, el Gateway genera un identificador aleatorio de 8 bytes (codificado en hexadecimal) al inicializar el `MessageHandler`. Ese identificador queda incorporado en todos los mensajes que el cliente envía al sistema —tanto mensajes de datos como el mensaje de fin de ingesta (EOF)— mediante un *envelope* JSON con la forma:

```json
{ "id": "<clientId>", "data": [[fruta, cantidad], ...] }
```

Todos los nodos del pipeline deserializan este *envelope* para obtener el identificador y tratar cada cliente como un flujo independiente.

---

## Escalado de instancias Sum

Las instancias de Sum comparten una única cola durable de RabbitMQ (cuyo nombre se configura con la variable de entorno `INPUT_QUEUE`). RabbitMQ distribuye los mensajes entre los consumidores activos con la política de *competing consumers*: cada instancia extrae un mensaje a la vez gracias al prefetch de QoS igual a 1, lo que reparte la carga de forma natural sin coordinación explícita.

Cada instancia acumula los datos recibidos en un mapa doble `clientMaps[clientId][fruta]` e incrementa los contadores locales de la suma. Dado que los mensajes de un mismo cliente pueden llegar a distintas instancias, cada Sum solo tiene una visión parcial de los datos de ese cliente; la consolidación final ocurre en el Aggregator.

---

## Propagación del EOF entre instancias Sum

Cuando una instancia de Sum recibe el EOF de un cliente, no puede proceder directamente a enviar sus parciales al Aggregator porque las demás instancias de Sum todavía podrían estar procesando mensajes de ese mismo cliente y nunca se enterarían del EOF. Para resolver esto se utiliza un **exchange de EOF dedicado** (`{SumPrefix}_eof`):

1. La instancia que detecta el EOF lo **reenvía (broadcast) al exchange de EOF** con todas las *routing keys* de las instancias Sum (`{SumPrefix}_0`, `{SumPrefix}_1`, …).
2. Cada instancia Sum tiene un consumidor propio suscrito a su *routing key* particular en ese exchange.
3. Cuando una instancia recibe el EOF a través de su consumidor de EOF, interpreta que el cliente terminó de enviar datos y debe volcar sus parciales al Aggregator.

Este diseño garantiza que **todas** las instancias de Sum procesen el EOF sin importar cuál de ellas lo recibió originalmente.

---

## Caso borde: mensaje en vuelo al llegar el EOF

Entre el momento en que una instancia Sum recibe el EOF por su exchange y el momento en que intenta volcar los datos, puede haber mensajes de datos de ese cliente que todavía están siendo procesados (mensajes *in flight*). Para manejarlo correctamente se implementa un esquema de sincronización con un mutex y un `sync.Cond`:

- **`globalPending`**: contador de mensajes que ya fueron extraídos de la cola principal pero todavía no fueron deserializados (en ese punto aún no se conoce el `clientId`).
- **`clientPending[clientId]`**: contador de mensajes de un cliente específico que están en pleno procesamiento.

El handler del EOF espera activamente (`cond.Wait`) hasta que ambos contadores lleguen a cero antes de llamar a `flushClient`. De ese modo, el volcado de parciales al Aggregator siempre ocurre con la acumulación local completamente estabilizada.

---

## Routing Sum → Aggregator (routing key por fruta)

Al volcar sus datos, cada instancia Sum no envía todos sus parciales a todos los Aggregators. En cambio, aplica un **hash FNV-32a sobre el nombre de la fruta** para determinar a qué instancia de Aggregator debe enviar cada par `(fruta, cantidad)`:

```
routingKey = aggregationKeys[ fnv32a(fruta) % len(aggregationKeys) ]
```

Esto garantiza que **todos los parciales de una misma fruta siempre lleguen al mismo Aggregator**, evitando procesamiento redundante y permitiendo que cada Aggregator consolide de forma independiente las frutas que le corresponden.

Una vez enviados todos los parciales, cada instancia Sum envía un mensaje de EOF —con el `clientId` correspondiente— a **todos** los Aggregators, ya que cada uno necesita saber que esa instancia Sum terminó.

---

## Coordinación de instancias Aggregator

Cada instancia de Aggregator recibe mensajes a través de un exchange directo de RabbitMQ. Su queue privada está ligada a la *routing key* que le asigna el Sum, por lo que solo recibe las frutas que le corresponden según el hash.

Para cada cliente, el Aggregator acumula los parciales en `clientMaps[clientId][fruta]` y lleva un contador de EOFs recibidos (`eofCount[clientId]`). Cuando ese contador alcanza la cantidad total de instancias Sum (`sumAmount`), el Aggregator sabe que recibió todos los parciales de ese cliente. En ese momento:

1. Ordena los ítems de mayor a menor y extrae el top parcial local.
2. Envía ese top —con el `clientId`— al Joiner a través de la cola de salida.
3. Libera la memoria de ese cliente.

---

## Consolidación en el Joiner

El Joiner recibe tops parciales de cada instancia de Aggregator. Por cada cliente lleva un contador `topCount[clientId]` y acumula todos los ítems recibidos en `clientTops[clientId]`. Cuando el contador alcanza el número total de Aggregators (`aggregationAmount`):

1. Ordena todos los ítems combinados de mayor a menor.
2. Toma los primeros `topSize` elementos como resultado final.
3. Serializa el top final —con el `clientId`— y lo envía a la cola de salida hacia el Gateway.
4. Libera la memoria de ese cliente.

---

## Entrega del resultado al cliente

El Gateway consume de la cola de salida del Joiner. Cada mensaje viene etiquetado con el `clientId` del cliente al que corresponde el resultado. Al recibir un mensaje, el Gateway itera sobre todos los clientes TCP actualmente conectados e intenta que cada uno lo procese:

1. Si un `MessageHandler` reconoce el `clientId` del mensaje como propio, extrae el top y lo entrega al cliente TCP. Luego envía un **ACK** a RabbitMQ para confirmar que el mensaje fue procesado y debe eliminarse de la cola.
2. Si ningún cliente conectado en ese momento puede procesar el mensaje (ninguno tiene ese `clientId`), se envía un **NACK** a RabbitMQ. En RabbitMQ, un NACK sin `requeue=false` devuelve el mensaje a la cola para que sea consumido nuevamente más adelante.

Esto permite que una única instancia de Gateway atienda múltiples clientes TCP concurrentes sin que un cliente reciba el resultado de otro.

---

## Resumen del flujo completo

```
Cliente TCP
    │  (fruta, cantidad) + clientId
    ▼
Gateway ──────────────────────────────────────────────────────┐
    │  cola Sum (competing consumers)                         │
    ▼                                                         │
Sum_0  Sum_1  …  Sum_N                                        │
    │  hash(fruta) → routing key                              │
    │  EOF → broadcast a todos los Sum vía exchange EOF       │
    ▼                                                         │
Exchange directo por routing key                              │
    │                                                         │
    ▼                                                         │
Aggregator_0  Aggregator_1  …  Aggregator_M                   │
    │  top parcial (cuando llegan sumAmount EOFs)             │
    ▼                                                         │
Cola Joiner                                                   │
    │                                                         │
    ▼                                                         │
Joiner  (cuando llegan aggregationAmount tops)                │
    │  top final                                              │
    ▼                                                         │
Cola Gateway ─────────────────────────────────────────────────┘
    │  filtrado por clientId
    ▼
Cliente TCP  ← resultado
```
