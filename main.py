import machine
import dht
import uasyncio as asyncio
import json
import binascii
import settings
from mqtt_as import MQTTClient, config

ID_DISPOSITIVO = binascii.hexlify(machine.unique_id()).decode()

# Vectores desacoplados
TOPIC_TELEMETRIA = f"{ID_DISPOSITIVO}/telemetria"
TOPIC_ESTADO = f"{ID_DISPOSITIVO}/estado"
TOPIC_COMANDO = f"{ID_DISPOSITIVO}/comando"

PIN_DHT = 15
PIN_RELE = 16
SENSOR_DHT = dht.DHT11(machine.Pin(PIN_DHT))
RELE = machine.Pin(PIN_RELE, machine.Pin.OUT, value=1) # Lógica inversa (1 = OFF)

memoria_dht = {"t": 0.0, "h": 0.0}

async def mantener_suscripciones(client):
    while True:
        await client.up.wait()
        client.up.clear()
        print(f"SYS: Enlace MQTTS -> {ID_DISPOSITIVO}")
        await client.subscribe(TOPIC_COMANDO, qos=1)

async def telemetria_periodica(client):
    while True:
        await client.up.wait()
        try:
            SENSOR_DHT.measure()
            memoria_dht["t"] = SENSOR_DHT.temperature()
            memoria_dht["h"] = SENSOR_DHT.humidity()
        except OSError:
            pass
            
        payload = json.dumps({
            "temperatura": memoria_dht["t"],
            "humedad": memoria_dht["h"]
        })
        
        await client.publish(TOPIC_TELEMETRIA, payload, qos=1)
        print(f"Tx [{TOPIC_TELEMETRIA}]: {payload}")
        await asyncio.sleep(10)

async def procesar_comandos(client):
    async for topic, msg, retained in client.queue:
        t = topic.decode()
        m = msg.decode().strip()
        print(f"Rx [{t}]: {m}")

        if t == TOPIC_COMANDO:
            if m == "true":
                RELE.value(0)
            elif m == "false":
                RELE.value(1)
            else:
                continue
            
            # Acuse de recibo plano para UI-LED
            estado_actual = "true" if RELE.value() == 0 else "false"
            await client.publish(TOPIC_ESTADO, estado_actual, qos=1)
            print(f"Tx [{TOPIC_ESTADO}]: {estado_actual}")

async def main():
    config['ssid'] = settings.SSID
    config['wifi_pw'] = settings.password
    config['server'] = settings.BROKER
    config['port'] = settings.MQTT_PORT
    config['user'] = settings.MQTT_USER
    config['password'] = settings.MQTT_PASS
    config['ssl'] = settings.MQTT_SSL
    config['queue_len'] = 15
    
    client = MQTTClient(config)

    asyncio.create_task(mantener_suscripciones(client))
    asyncio.create_task(telemetria_periodica(client))
    asyncio.create_task(procesar_comandos(client))

    try:
        await client.connect()
    except OSError as e:
        print(f"FATAL: Socket/DNS -> {e}")

    while True:
        await asyncio.sleep(1)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass