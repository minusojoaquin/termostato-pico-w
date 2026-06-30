import machine
import time
import dht
import json
import network
from umqtt.simple import MQTTClient
import settings

# 1. Inicialización de Capa de Red (WLAN)
def inicializar_wlan():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    if not wlan.isconnected():
        print(f"SYS: Asociando a SSID [{settings.SSID}]...")
        wlan.connect(settings.SSID, settings.password)
        while not wlan.isconnected():
            time.sleep(1)
    print(f"SYS: Enlace establecido. IP asignada: {wlan.ifconfig()[0]}")

inicializar_wlan()

# 2. Definición de Hardware
rele = machine.Pin(2, machine.Pin.OUT)
sensor = dht.DHT11(machine.Pin(4))

# 3. Definición de Tópicos MQTT
TOPIC_SUB_COMANDO = b"comando"
TOPIC_PUB_ESTADO = b"estado"
TOPIC_PUB_TELEMETRIA = b"Sensores"

# 4. Callback para recibir comandos
def callback_rutina(topic, msg):
    comando = msg.decode('utf-8')
    print("Rx [comando]:", comando)
    
    if comando == "true":
        rele.value(1)
    elif comando == "false":
        rele.value(0)
        
    estado_actual = "true" if rele.value() == 1 else "false"
    print("Tx [estado]:", estado_actual)
    cliente.publish(TOPIC_PUB_ESTADO, estado_actual.encode())

# 5. Configuración MQTT
cliente = MQTTClient(
    client_id="nodoremoto",
    server=settings.BROKER,
    port=settings.MQTT_PORT,
    user=settings.MQTT_USER,
    password=settings.MQTT_PASS,
    ssl=settings.MQTT_SSL
)

cliente.set_callback(callback_rutina)
cliente.connect()
cliente.subscribe(TOPIC_SUB_COMANDO)
print("SYS: Conectado a Broker MQTT.")

# 6. Bucle de Telemetría (No Bloqueante)
intervalo_envio = 5000 
ultimo_envio = time.ticks_ms()

try:
    while True:
        cliente.check_msg()
        ahora = time.ticks_ms()
        
        if time.ticks_diff(ahora, ultimo_envio) > intervalo_envio:
            try:
                sensor.measure()
                payload = json.dumps({"temperatura": sensor.temperature(), "humedad": sensor.humidity()})
                cliente.publish(TOPIC_PUB_TELEMETRIA, payload.encode())
                print(f"Tx [{TOPIC_PUB_TELEMETRIA.decode()}]: {payload}")
            except OSError:
                print("SYS: Error de lectura en sensor DHT.")
                
            ultimo_envio = time.ticks_ms()
            
        time.sleep(0.1)

except KeyboardInterrupt:
    cliente.disconnect()