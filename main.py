import machine
import dht
import uasyncio as asyncio
import json
import binascii
import settings

from mqtt_local import config
from mqtt_as import MQTTClient

# Asignación de Pines de Hardware
PIN_DHT = 15
PIN_RELE = 14

SENSOR_DHT = dht.DHT11(machine.Pin(PIN_DHT))
RELE = machine.Pin(PIN_RELE, machine.Pin.OUT, value=1)
LED = machine.Pin("LED", machine.Pin.OUT)
ID_DISPOSITIVO = binascii.hexlify(machine.unique_id()).decode()
ARCHIVO_ESTADO = "estado.json"
evento_cambio = asyncio.Event()

def cargar_estado():
    try:
        with open(ARCHIVO_ESTADO, "r") as f:
            return json.load(f)
    except OSError:
        return {'setpoint': 25.0, 'periodo': 10, 'modo': 'auto', 'rele': 0}

def guardar_estado():
    datos = {
        'setpoint': estado['setpoint'],
        'periodo': estado['periodo'],
        'modo': estado['modo'],
        'rele': estado['rele']
    }
    try:
        with open(ARCHIVO_ESTADO, "w") as f:
            json.dump(datos, f)
    except OSError:
        print("Falla de I/O Flash")

estado = cargar_estado()
estado['temperatura'] = 0.0
estado['humedad'] = 0.0

async def destello():
    print("Secuencia de destello iniciada...")
    for _ in range(10):
        LED.toggle()
        await asyncio.sleep_ms(200)
    LED.off()

async def control_termostato():
    while True:
        try:
            SENSOR_DHT.measure()
            estado['temperatura'] = SENSOR_DHT.temperature()
            estado['humedad'] = SENSOR_DHT.humidity()
        except OSError as e:
            print(f"[!] Falla de hardware en SENSOR_DHT: {e}")
        
        if estado['modo'] == 'auto':
            if estado['temperatura'] > estado['setpoint']:
                RELE.value(0)
            else:
                RELE.value(1)
        elif estado['modo'] == 'manual':
            RELE.value(0 if estado['rele'] == 1 else 1)

        try:
            await asyncio.wait_for(evento_cambio.wait(), 3)
            evento_cambio.clear()
            print("[Modo Asíncrono] Cambio detectado por MQTT: Interrupción ejecutada.")
        except asyncio.TimeoutError:
            pass

async def publicar_estado(client):
    while True:
        await client.up.wait()
        payload = json.dumps({
            "temperatura": estado['temperatura'],
            "humedad": estado['humedad'],
            "setpoint": estado['setpoint'],
            "periodo": estado['periodo'],
            "modo": estado['modo']
        })
        await client.publish(ID_DISPOSITIVO, payload, qos=1)
        await asyncio.sleep(estado['periodo'])

async def procesar_eventos_mqtt(client):
    async for topic, msg, retained in client.queue:
        try:
            t = topic.decode()
            m = msg.decode().strip()
            print(f"Rx -> Topic: {t} | Payload: {m}")
            
            if t.endswith("/setpoint"):
                estado['setpoint'] = float(m)
                guardar_estado()
                evento_cambio.set()  
            elif t.endswith("/periodo"):
                estado['periodo'] = int(m)
                guardar_estado()
            elif t.endswith("/modo"):
                estado['modo'] = m
                guardar_estado()
                evento_cambio.set()  
            elif t.endswith("/rele"):
                estado['rele'] = int(m)
                guardar_estado()
                evento_cambio.set()  
            elif t.endswith("/destello"):
                asyncio.create_task(destello())
        except Exception as e:
            print(f"Falla de decodificación: {e}")

async def conexion_broker(client):
    while True:
        await client.up.wait()
        client.up.clear()
        print(f"\n[!] Conexion MQTTS Establecido. ID de placa: {ID_DISPOSITIVO}")
        
        topicos = ["/setpoint", "/periodo", "/destello", "/modo", "/rele"]
        for sub in topicos:
            await client.subscribe(ID_DISPOSITIVO + sub, qos=1)
            print(f"Suscrito a tópico de escucha: {ID_DISPOSITIVO + sub}")
        
        await client.down.wait()
        client.down.clear()
        print("\n[X] Conexión perdida. Intentando restaurar conexion...")

async def main():
    config['ssid'] = settings.SSID
    config['wifi_pw'] = settings.password
    config['server'] = settings.BROKER
    config['port'] = settings.MQTT_PORT
    config['user'] = settings.MQTT_USER
    config['password'] = settings.MQTT_PASS
    config['ssl'] = settings.MQTT_SSL
    config['queue_len'] = settings.MQTT_QUEUE
    
    client = MQTTClient(config)

    asyncio.create_task(conexion_broker(client))
    asyncio.create_task(control_termostato())
    asyncio.create_task(publicar_estado(client))
    asyncio.create_task(procesar_eventos_mqtt(client))

    try:
        await client.connect()
    except OSError as e:
        print(f"Error crítico de red Wi-Fi/DNS: {e}")

    while True:
        await asyncio.sleep(1)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass