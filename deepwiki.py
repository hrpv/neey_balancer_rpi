"""  
heltec_balancer_ble.py  
Python port of components/heltec_balancer_ble/heltec_balancer_ble.cpp  
Requires: pip install bleak  
"""  
  
import asyncio  
import struct  
import logging  
from bleak import BleakClient, BleakScanner  
  
log = logging.getLogger(__name__)  
  
# ── Protocol constants ────────────────────────────────────────────────────────  
HELTEC_SERVICE_UUID        = "0000FFE0-0000-1000-8000-00805F9B34FB"  
HELTEC_CHARACTERISTIC_UUID = "0000FFE1-0000-1000-8000-00805F9B34FB"  
  
SOF_REQUEST_BYTE1  = 0xAA  
SOF_REQUEST_BYTE2  = 0x55  
SOF_RESPONSE_BYTE1 = 0x55  
SOF_RESPONSE_BYTE2 = 0xAA  
DEVICE_ADDRESS     = 0x11  
  
FUNCTION_WRITE = 0x00  
FUNCTION_READ  = 0x01  
  
COMMAND_NONE             = 0x00  
COMMAND_DEVICE_INFO      = 0x01  
COMMAND_CELL_INFO        = 0x02  
COMMAND_FACTORY_DEFAULTS = 0x03  
COMMAND_SETTINGS         = 0x04  
COMMAND_WRITE_REGISTER   = 0x05  
  
END_OF_FRAME      = 0xFF  
MIN_RESPONSE_SIZE = 20  
MAX_RESPONSE_SIZE = 300  
  
OPERATION_STATUS = [  
    "Unknown",  
    "Wrong cell count",  
    "AcqLine Res test",  
    "AcqLine Res exceed",  
    "Systest Completed",  
    "Balancing",  
    "Balancing finished",  
    "Low voltage",  
    "System Overtemp",  
    "Host fails",  
    "Low battery voltage - balancing stopped",  
    "Temperature too high - balancing stopped",  
    "Self-test completed",  
]  
  
BUZZER_MODES   = ["Unknown", "Off", "Beep once", "Beep regular"]  
BATTERY_TYPES  = ["Unknown", "NCM", "LFP", "LTO", "PbAc"]  
  
  
# ── Helpers ───────────────────────────────────────────────────────────────────  
  
def crc(data: bytes | bytearray, length: int) -> int:  
    """Simple additive checksum (sum of bytes, truncated to uint8)."""  
    return sum(data[:length]) & 0xFF  
  
  
def ieee_float(raw32: int) -> float:  
    """Reinterpret a uint32 as an IEEE-754 single-precision float."""  
    return struct.unpack("<f", struct.pack("<I", raw32))[0]  
  
  
def get_16bit(data: bytes | bytearray, i: int) -> int:  
    return (data[i + 1] << 8) | data[i]  
  
  
def get_24bit(data: bytes | bytearray, i: int) -> int:  
    return (data[i + 2] << 16) | (data[i + 1] << 8) | data[i]  
  
  
def get_32bit(data: bytes | bytearray, i: int) -> int:  
    return (get_16bit(data, i + 2) << 16) | get_16bit(data, i)  
  
  
def format_total_runtime(seconds: int) -> str:  
    years = seconds // (24 * 3600 * 365)  
    seconds %= 24 * 3600 * 365  
    days  = seconds // (24 * 3600)  
    seconds %= 24 * 3600  
    hours = seconds // 3600  
    parts = []  
    if years: parts.append(f"{years}y")  
    if days:  parts.append(f"{days}d")  
    if hours: parts.append(f"{hours}h")  
    return " ".join(parts) or "0h"  
  
  
# ── Frame decoders ────────────────────────────────────────────────────────────  
  
def decode_device_info(data: bytearray) -> dict:  
    result = {}  
    result["model"]            = data[8:24].rstrip(b"\x00").decode("ascii", errors="replace")  
    result["hw_version"]       = data[24:32].rstrip(b"\x00").decode("ascii", errors="replace")  
    result["sw_version"]       = data[32:40].rstrip(b"\x00").decode("ascii", errors="replace")  
    result["protocol_version"] = data[40:48].rstrip(b"\x00").decode("ascii", errors="replace")  
    result["manufacture_date"] = data[48:56].rstrip(b"\x00").decode("ascii", errors="replace")  
    result["power_on_count"]   = get_16bit(data, 56)  
    total_s                    = get_32bit(data, 60)  
    result["total_runtime_s"]  = total_s  
    result["total_runtime_fmt"]= format_total_runtime(total_s)  
    log.info("Model: %s  HW: %s  SW: %s  Proto: %s  Mfg: %s  PowerOnCount: %d  Runtime: %s",  
             result["model"], result["hw_version"], result["sw_version"],  
             result["protocol_version"], result["manufacture_date"],  
             result["power_on_count"], result["total_runtime_fmt"])  
    return result  
  
  
def decode_cell_info(data: bytearray) -> dict:  
    result = {}  
    log.debug("Frame counter: %d", data[8])  
  
    # Cell voltages  (bytes 9–104, 4 bytes each, 24 cells)  
    # Cell resistances (bytes 105–200, 4 bytes each, 24 cells)  
    cells = []  
    min_v, max_v = 1e9, -1e9  
    min_cell = max_cell = 0  
    total_v = 0.0  
    enabled = 0  
  
    for i in range(24):  
        voltage    = ieee_float(get_32bit(data, i * 4 + 9))  
        resistance = ieee_float(get_32bit(data, i * 4 + 105))  
        cells.append({"voltage": voltage, "resistance": resistance})  
  
        if voltage > 0:  
            total_v += voltage  
            enabled += 1  
            if voltage < min_v:  
                min_v = voltage  
                min_cell = i + 1  
        if voltage > max_v:  
            max_v = voltage  
            max_cell = i + 1  
  
    result["cells"]               = cells  
    result["min_cell_voltage"]    = min_v  if enabled else float("nan")  
    result["max_cell_voltage"]    = max_v  if enabled else float("nan")  
    result["min_voltage_cell"]    = min_cell  
    result["max_voltage_cell"]    = max_cell  
    result["delta_cell_voltage"]  = (max_v - min_v) if enabled else float("nan")  
    result["average_cell_voltage"]= (total_v / enabled) if enabled else float("nan")  
  
    # Total voltage (byte 201)  
    result["total_voltage"]       = ieee_float(get_32bit(data, 201))  
  
    # Operation status (byte 216)  
    raw_status = data[216]  
    result["balancing"]           = (raw_status == 0x05)  
    result["operation_status"]    = (OPERATION_STATUS[raw_status]  
                                     if raw_status < len(OPERATION_STATUS)  
                                     else "Unknown")  
  
    # Balancing current (bytes 217–220)  
    result["balancing_current"]   = ieee_float(get_32bit(data, 217))  
  
    # Temperatures (bytes 221–228)  
    result["temperature_1"]       = ieee_float(get_32bit(data, 221))  
    result["temperature_2"]       = ieee_float(get_32bit(data, 225))  
  
    # Error bitmasks (bytes 229–246)  
    result["cell_detection_failed_bitmask"]        = get_24bit(data, 229)  
    result["cell_overvoltage_bitmask"]             = get_24bit(data, 232)  
    result["cell_undervoltage_bitmask"]            = get_24bit(data, 235)  
    result["cell_polarity_error_bitmask"]          = get_24bit(data, 238)  
    result["cell_excessive_line_resistance_bitmask"] = get_24bit(data, 241)  
    result["error_system_overheating"]             = data[244] != 0x00  
    result["error_charging"]                       = bool(data[245])  
    result["error_discharging"]                    = bool(data[246])  
  
    # Uptime (bytes 254–257)  
    uptime = get_32bit(data, 254)  
    result["uptime_s"]            = uptime  
    result["uptime_fmt"]          = format_total_runtime(uptime)  
  
    log.info("Operation status: %s  Balancing: %s  Total voltage: %.3f V  "  
             "Temp1: %.1f°C  Temp2: %.1f°C  Uptime: %s",  
             result["operation_status"], result["balancing"],  
             result["total_voltage"], result["temperature_1"],  
             result["temperature_2"], result["uptime_fmt"])  
    return result  
  
  
def decode_settings(data: bytearray) -> dict:  
    result = {}  
    result["cell_count"]             = data[8]  
    result["balance_trigger_voltage"]= ieee_float(get_32bit(data, 9))  
    result["max_balance_current"]    = ieee_float(get_32bit(data, 13))  
    result["balance_sleep_voltage"]  = ieee_float(get_32bit(data, 17))  
    result["balancer_enabled"]       = bool(data[21])  
    raw_buzzer = data[22]  
    result["buzzer_mode"]            = (BUZZER_MODES[raw_buzzer]  
                                        if raw_buzzer < len(BUZZER_MODES)  
                                        else "Unknown")  
    raw_batt = data[23]  
    result["battery_type"]           = (BATTERY_TYPES[raw_batt]  
                                        if raw_batt < len(BATTERY_TYPES)  
                                        else "Unknown")  
    result["nominal_battery_capacity"]= get_32bit(data, 24)  
    result["balance_start_voltage"]  = ieee_float(get_32bit(data, 28))  
    log.info("Settings: cells=%d  trig=%.4fV  maxI=%.2fA  sleepV=%.2fV  "  
             "enabled=%s  buzzer=%s  battery=%s  cap=%d  startV=%.2fV",  
             result["cell_count"], result["balance_trigger_voltage"],  
             result["max_balance_current"], result["balance_sleep_voltage"],  
             result["balancer_enabled"], result["buzzer_mode"],  
             result["battery_type"], result["nominal_battery_capacity"],  
             result["balance_start_voltage"])  
    return result  
  
  
def decode_factory_defaults(data: bytearray) -> dict:  
    if len(data) == 20:           # acknowledge frame only  
        return {}  
    result = {}  
    result["standard_voltage_1"] = ieee_float(get_32bit(data, 8))  
    result["standard_voltage_2"] = ieee_float(get_32bit(data, 12))  
    result["battery_voltage_1"]  = ieee_float(get_32bit(data, 16))  
    result["battery_voltage_2"]  = ieee_float(get_32bit(data, 20))  
    result["standard_current_1"] = ieee_float(get_32bit(data, 24))  
    result["standard_current_2"] = ieee_float(get_32bit(data, 28))  
    result["superbat_1"]         = ieee_float(get_32bit(data, 32))  
    result["superbat_2"]         = ieee_float(get_32bit(data, 36))  
    result["resistor_1"]         = ieee_float(get_32bit(data, 40))  
    result["battery_status"]     = ieee_float(get_32bit(data, 44))  
    result["max_voltage"]        = ieee_float(get_32bit(data, 48))  
    result["min_voltage"]        = ieee_float(get_32bit(data, 52))  
    result["max_temperature"]    = ieee_float(get_32bit(data, 56))  
    result["min_temperature"]    = ieee_float(get_32bit(data, 60))  
    result["power_on_counter"]   = get_32bit(data, 64)  
    result["total_runtime"]      = get_32bit(data, 68)  
    result["production_date"]    = data[72:80].rstrip(b"\x00").decode("ascii", errors="replace")  
    return result  
  
  
# ── Command builder ───────────────────────────────────────────────────────────  
  
def build_command(function: int, command: int,  
                  register_address: int = 0x00,  
                  value: int = 0x00000000) -> bytes:  
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
    # bytes 12-17 are 0x00  
    frame[18] = crc(frame, 18)  
    frame[19] = END_OF_FRAME  
    return bytes(frame)  
  
  
# ── Frame assembler / dispatcher ──────────────────────────────────────────────  
  
class HeltecBalancerBle:  
    def __init__(self):  
        self._frame_buffer = bytearray()  
  
    def assemble(self, chunk: bytes | bytearray) -> dict | None:  
        """Feed a BLE notification chunk; returns decoded dict when a full  
        valid frame is received, otherwise None."""  
        if len(self._frame_buffer) > MAX_RESPONSE_SIZE:  
            log.warning("Frame dropped – buffer too large")  
            self._frame_buffer.clear()  
  
        if len(chunk) >= 2 and chunk[0] == SOF_RESPONSE_BYTE1 and chunk[1] == SOF_RESPONSE_BYTE2:  
            self._frame_buffer.clear()  
  
        self._frame_buffer.extend(chunk)  
  
        if (len(self._frame_buffer) >= MIN_RESPONSE_SIZE  
                and self._frame_buffer[-1] == END_OF_FRAME):  
  
            frame  = bytes(self._frame_buffer)  
            size   = len(frame)  
            computed = crc(frame, size - 2)  
            remote   = frame[size - 2]  
  
            if computed != remote:  
                log.warning("CRC mismatch: 0x%02X != 0x%02X", computed, remote)  
                self._frame_buffer.clear()  
                return None  
  
            result = self._decode(bytearray(frame))  
            self._frame_buffer.clear()  
            return result  
  
        return None  
  
    def _decode(self, data: bytearray) -> dict | None:  
        frame_type = data[4]  
        if frame_type == COMMAND_DEVICE_INFO:  
            return {"type": "device_info",      **decode_device_info(data)}  
        elif frame_type == COMMAND_CELL_INFO:  
            return {"type": "cell_info",        **decode_cell_info(data)}  
        elif frame_type == COMMAND_FACTORY_DEFAULTS:  
            return {"type": "factory_defaults", **decode_factory_defaults(data)}  
        elif frame_type == COMMAND_SETTINGS:  
            return {"type": "settings",         **decode_settings(data)}  
        elif frame_type == COMMAND_WRITE_REGISTER:  
            log.debug("Write-register ACK received")  
            return {"type": "write_ack"}  
        else:  
            log.warning("Unknown frame type: 0x%02X", frame_type)  
            return None  
  
  
# ── BLE client entry-point ────────────────────────────────────────────────────  
  
async def run(mac_address: str):  
    logging.basicConfig(level=logging.INFO)  
    balancer = HeltecBalancerBle()  
  
    async with BleakClient(mac_address) as client:  
        log.info("Connected to %s", mac_address)  
  
        def notification_handler(_, data: bytearray):  
            result = balancer.assemble(data)  
            if result:  
                log.info("Decoded frame: %s", result.get("type"))  
                # ── add your own publish/MQTT/HA logic here ──  
  
        await client.start_notify(HELTEC_CHARACTERISTIC_UUID, notification_handler)  
  
        # Request device info then poll cell info  
        await client.write_gatt_char(  
            HELTEC_CHARACTERISTIC_UUID,  
            build_command(FUNCTION_READ, COMMAND_DEVICE_INFO),  
            response=False,  
        )  
        await asyncio.sleep(2)  
  
        while True:  
            await client.write_gatt_char(  
                HELTEC_CHARACTERISTIC_UUID,  
                build_command(FUNCTION_READ, COMMAND_CELL_INFO),  
                response=False,  
            )  
            await asyncio.sleep(30)  
  
  
if __name__ == "__main__":  
    import sys  
    if len(sys.argv) < 2:  
        print("Usage: python heltec_balancer_ble.py <MAC_ADDRESS>")  
        sys.exit(1)  
    asyncio.run(run(sys.argv[1]))
