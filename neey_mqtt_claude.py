#!/usr/bin/env python3
"""
NEEY Balancer MQTT Service
Connects via BLE every 30 seconds, fetches cell data, disconnects, publishes to MQTT
"""

import asyncio
import struct
import logging
import sys
import json
from datetime import datetime
from bleak import BleakClient
import paho.mqtt.client as mqtt

# ── Configuration ─────────────────────────────────────────────────────────────
BALANCER_MAC = "3C:A5:51:95:81:72"  # Change to your MAC
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "NEEY"
UPDATE_INTERVAL = 30  # seconds between BLE connections

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# ── Protocol constants ────────────────────────────────────────────────────────
HELTEC_SERVICE_UUID        = "0000FFE0-0000-1000-8000-00805F9B34FB"
HELTEC_CHARACTERISTIC_UUID = "0000FFE1-0000-1000-8000-00805F9B34FB"

SOF_REQUEST_BYTE1  = 0xAA
SOF_REQUEST_BYTE2  = 0x55
SOF_RESPONSE_BYTE1 = 0x55
SOF_RESPONSE_BYTE2 = 0xAA
DEVICE_ADDRESS     = 0x11

FUNCTION_READ       = 0x01
COMMAND_DEVICE_INFO = 0x01
COMMAND_CELL_INFO   = 0x02

END_OF_FRAME      = 0xFF
MIN_RESPONSE_SIZE = 20
MAX_RESPONSE_SIZE = 300

# ── MQTT Client ───────────────────────────────────────────────────────────────
mqtt_client = None

def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("MQTT connected to %s:%d", MQTT_BROKER, MQTT_PORT)
    else:
        log.error("MQTT connection failed with code %s", reason_code)

def on_mqtt_disconnect(client, userdata, flags, reason_code, properties):
    log.warning("MQTT disconnected (code %s), will retry...", reason_code)

def init_mqtt():
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_disconnect = on_mqtt_disconnect

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        return True
    except Exception as e:
        log.error("MQTT init failed: %s", e)
        return False

def publish_data(data: dict):
    """Publish data to MQTT"""
    if mqtt_client is None or not mqtt_client.is_connected():
        log.warning("MQTT not connected, skipping publish")
        return

    try:
        payload = {
            "timestamp": datetime.now().isoformat(),
            "device": {
                "model": data.get("model", "Unknown"),
                "hw_version": data.get("hw_version", ""),
                "sw_version": data.get("sw_version", ""),
            },
            "battery": {
                "total_voltage": round(data.get("total_voltage", 0), 3),
                "average_cell_voltage": round(data.get("average_cell_voltage", 0), 3),
                "min_cell_voltage": round(data.get("min_cell_voltage", 0), 3),
                "max_cell_voltage": round(data.get("max_cell_voltage", 0), 3),
                "delta_voltage": round(data.get("delta_cell_voltage", 0), 3),
                "cell_count": data.get("cell_count", 0),
                "temperature_1": round(data.get("temperature_1", 0), 1),
                "temperature_2": round(data.get("temperature_2", 0), 1),
                "balancing": data.get("balancing", False),
                "status": data.get("operation_status", "Unknown"),
            },
            "cells": []
        }

        for i, cell in enumerate(data.get("cells", []), 1):
            if cell["voltage"] > 0:
                payload["cells"].append({
                    "cell": i,
                    "voltage": round(cell["voltage"], 3),
                    "resistance": round(cell["resistance"], 3)
                })

        mqtt_client.publish(f"{MQTT_TOPIC}/data", json.dumps(payload), qos=1, retain=True)
        mqtt_client.publish(f"{MQTT_TOPIC}/total_voltage", payload["battery"]["total_voltage"], qos=1, retain=True)
        mqtt_client.publish(f"{MQTT_TOPIC}/delta_voltage", payload["battery"]["delta_voltage"], qos=1, retain=True)
        mqtt_client.publish(f"{MQTT_TOPIC}/temperature", payload["battery"]["temperature_1"], qos=1, retain=True)
        mqtt_client.publish(f"{MQTT_TOPIC}/balancing", "ON" if payload["battery"]["balancing"] else "OFF", qos=1, retain=True)

        for cell in payload["cells"]:
            mqtt_client.publish(f"{MQTT_TOPIC}/cell_{cell['cell']}/voltage", cell["voltage"], qos=1, retain=True)

        log.info("Published to MQTT topic %s", MQTT_TOPIC)

    except Exception as e:
        log.error("MQTT publish failed: %s", e)

# ── Protocol helpers ──────────────────────────────────────────────────────────
def crc(data, length):
    return sum(data[:length]) & 0xFF

def ieee_float(raw32):
    return struct.unpack("<f", struct.pack("<I", raw32))[0]

def get_16bit(data, i):
    return (data[i + 1] << 8) | data[i]

def get_32bit(data, i):
    return (get_16bit(data, i + 2) << 16) | get_16bit(data, i)

def build_command(function, command, register_address=0x00, value=0x00000000):
    length = 0x0014
    frame = bytearray(20)
    frame[0]  = SOF_REQUEST_BYTE1
    frame[1]  = SOF_REQUEST_BYTE2
    frame[2]  = DEVICE_ADDRESS
    frame[3]  = function
    frame[4]  = command & 0xFF
    frame[5]  = register_address
    frame[6]  = length & 0xFF
    frame[7]  = (length >> 8) & 0xFF
    frame[8]  = (value >> 0)  & 0xFF
    frame[9]  = (value >> 8)  & 0xFF
    frame[10] = (value >> 16) & 0xFF
    frame[11] = (value >> 24) & 0xFF
    frame[18] = crc(frame, 18)
    frame[19] = END_OF_FRAME
    return bytes(frame)

# ── Frame decoder ─────────────────────────────────────────────────────────────
class HeltecBalancerBle:
    def __init__(self):
        self._frame_buffer = bytearray()
        self.device_info = {}

    def assemble(self, chunk):
        if len(self._frame_buffer) > MAX_RESPONSE_SIZE:
            self._frame_buffer.clear()

        if len(chunk) >= 2 and chunk[0] == SOF_RESPONSE_BYTE1 and chunk[1] == SOF_RESPONSE_BYTE2:
            self._frame_buffer.clear()

        self._frame_buffer.extend(chunk)

        if (len(self._frame_buffer) >= MIN_RESPONSE_SIZE and
                self._frame_buffer[-1] == END_OF_FRAME):
            frame = bytes(self._frame_buffer)
            size = len(frame)
            computed = crc(frame, size - 2)
            remote = frame[size - 2]

            if computed != remote:
                log.warning("CRC mismatch")
                self._frame_buffer.clear()
                return None

            result = self._decode(bytearray(frame))
            self._frame_buffer.clear()
            return result

        return None

    def _decode(self, data):
        frame_type = data[4]

        if frame_type == COMMAND_DEVICE_INFO:
            self.device_info = {
                "model": data[8:24].rstrip(b"\x00").decode("ascii", errors="replace"),
                "hw_version": data[24:32].rstrip(b"\x00").decode("ascii", errors="replace"),
                "sw_version": data[32:40].rstrip(b"\x00").decode("ascii", errors="replace"),
            }
            log.info("Device: %s HW:%s SW:%s",
                     self.device_info["model"],
                     self.device_info["hw_version"],
                     self.device_info["sw_version"])
            return None

        elif frame_type == COMMAND_CELL_INFO:
            cells = []
            min_v, max_v = float('inf'), float('-inf')
            total_v = 0.0
            active = 0

            for i in range(24):
                voltage = ieee_float(get_32bit(data, i * 4 + 9))
                resistance = ieee_float(get_32bit(data, i * 4 + 105))
                cells.append({"voltage": voltage, "resistance": resistance})

                if voltage > 0:
                    total_v += voltage
                    active += 1
                    min_v = min(min_v, voltage)
                    max_v = max(max_v, voltage)

            result = {
                "type": "cell_info",
                "cells": cells,
                "cell_count": active,
                "total_voltage": ieee_float(get_32bit(data, 201)),
                "average_cell_voltage": total_v / active if active else 0,
                "min_cell_voltage": min_v if active else 0,
                "max_cell_voltage": max_v if active else 0,
                "delta_cell_voltage": (max_v - min_v) if active else 0,
                "temperature_1": ieee_float(get_32bit(data, 221)),
                "temperature_2": ieee_float(get_32bit(data, 225)),
                "balancing": data[216] == 0x05,
                "operation_status": data[216],
            }
            result.update(self.device_info)
            return result

        return None

# ── BLE helpers ───────────────────────────────────────────────────────────────
async def ble_request(command: int, on_notification) -> bool:
    """
    Open a fresh BLE connection, send one command, wait until on_notification()
    returns True (frame complete) or the device disconnects, then close.
    The EOFError from dbus_fast when the device drops the link is swallowed.
    """
    done = asyncio.Event()

    def _notif(_, data):
        if on_notification(data):
            done.set()

    def _disconnected(_):
        log.warning("BLE device disconnected unexpectedly")
        done.set()

    log.info("BLE connecting for command 0x%02X ...", command)
    try:
        async with BleakClient(
            BALANCER_MAC,
            disconnected_callback=_disconnected,
        ) as client:
            log.info("BLE connected")
            await client.start_notify(HELTEC_CHARACTERISTIC_UUID, _notif)
            await client.write_gatt_char(
                HELTEC_CHARACTERISTIC_UUID,
                build_command(FUNCTION_READ, command),
                response=False,
            )
            try:
                await asyncio.wait_for(done.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                log.warning("Timeout waiting for command 0x%02X", command)

            if client.is_connected:
                try:
                    await client.stop_notify(HELTEC_CHARACTERISTIC_UUID)
                except Exception:
                    pass
    except EOFError:
        # dbus_fast raises EOFError when the device closes the BT link – harmless
        log.debug("EOFError on disconnect for command 0x%02X (device closed link)", command)
    except Exception as e:
        log.error("BLE error for command 0x%02X: %s", command, e, exc_info=True)
        return False

    log.info("BLE session for command 0x%02X done", command)
    return True


# ── Single fetch cycle ────────────────────────────────────────────────────────
async def fetch_once(balancer: HeltecBalancerBle) -> dict | None:
    """
    The balancer drops the BLE link immediately after each response, so each
    command gets its own fresh connection.

    Cycle:
      1. Connect -> COMMAND_DEVICE_INFO -> device disconnects -> we reconnect
      2. Connect -> COMMAND_CELL_INFO   -> device disconnects -> done
    Device info is cached after the first successful read.
    """

    # ── Step 1: device info (once) ────────────────────────────────────────────
    if not balancer.device_info:
        def on_device_info(chunk):
            balancer.assemble(chunk)
            return bool(balancer.device_info)

        ok = await ble_request(COMMAND_DEVICE_INFO, on_device_info)
        if not ok or not balancer.device_info:
            log.warning("Could not read device info – aborting this cycle")
            return None

        # Give the radio a moment before opening the next connection
        await asyncio.sleep(2.0)
    else:
        log.debug("Device info cached: %s", balancer.device_info.get("model"))

    # ── Step 2: cell info ─────────────────────────────────────────────────────
    result_holder: dict = {}

    def on_cell_info(chunk):
        result = balancer.assemble(chunk)
        if result and result.get("type") == "cell_info":
            result_holder["data"] = result
            return True
        return False

    await ble_request(COMMAND_CELL_INFO, on_cell_info)
    return result_holder.get("data")

# ── Main loop ─────────────────────────────────────────────────────────────────
async def main():
    if not init_mqtt():
        log.error("Failed to initialize MQTT, exiting")
        sys.exit(1)

    balancer = HeltecBalancerBle()

    while True:
        try:
            result = await fetch_once(balancer)

            if result:
                print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - "
                      f"Total: {result['total_voltage']:.2f}V "
                      f"Delta: {result['delta_cell_voltage']:.3f}V "
                      f"Cells: {result['cell_count']}")
                publish_data(result)
            else:
                log.warning("No cell data received in this cycle")

        except Exception as e:
            log.error("BLE fetch error: %s", e, exc_info=True)

        log.info("Sleeping %d s until next BLE poll ...", UPDATE_INTERVAL)
        await asyncio.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down...")
        if mqtt_client:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            