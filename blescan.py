#!/usr/bin/env python3
"""
NEEY Balancer BLE Scanner for Raspberry Pi
Scans for NEEY/Heltec/GiantKey/EnerKey balancers
"""

import asyncio
from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
import sys

# Known NEEY/Heltec/GiantKey/EnerKey name prefixes
NEEY_PREFIXES = [
    "GW-24S",    # NEEY 4th gen (most common)
    "GW-16S",
    "GW-8S",
    "GW-",
    "EK-24S",    # EnerKey variants
    "EK-16S",
    "GK-24S",    # GiantKey variants
    "Heltec",
    "NEEY",
]

# Known service UUIDs for NEEY balancers
NEEY_SERVICE_UUIDS = [
    "0000ffe0-0000-1000-8000-00805f9b34fb",  # Standard UART service
]


def is_neey_balancer(device: BLEDevice, adv: AdvertisementData) -> bool:
    """Check if device is a NEEY balancer"""
    name = device.name or ""
    
    # Check name prefixes
    for prefix in NEEY_PREFIXES:
        if name.startswith(prefix):
            return True
    
    # Check service UUIDs
    if adv.service_uuids:
        for uuid in adv.service_uuids:
            if uuid in NEEY_SERVICE_UUIDS:
                return True
    
    return False


async def scan_neey(timeout: float = 15.0):
    """Scan for NEEY balancers"""
    print("=" * 60)
    print("NEEY Balancer BLE Scanner")
    print("=" * 60)
    print(f"Scanning for {timeout} seconds...")
    print(f"Looking for prefixes: {', '.join(NEEY_PREFIXES)}")
    print("-" * 60)
    
    found_devices = []
    
    def detection_callback(device: BLEDevice, adv: AdvertisementData):
        if is_neey_balancer(device, adv):
            if device.address not in [d.address for d in found_devices]:
                found_devices.append(device)
                print(f"\n🎯 NEEY BALANCER FOUND!")
                print(f"   Name:        {device.name or 'Unknown'}")
                print(f"   MAC Address: {device.address}")
                print(f"   RSSI:        {adv.rssi} dBm (signal strength)")
                
                # Print service UUIDs
                if adv.service_uuids:
                    print(f"   Service UUIDs:")
                    for uuid in adv.service_uuids:
                        print(f"      - {uuid}")
                
                # Print manufacturer data if available
                if adv.manufacturer_data:
                    print(f"   Manufacturer Data:")
                    for company_id, data in adv.manufacturer_data.items():
                        print(f"      Company ID: 0x{company_id:04X}")
                        print(f"      Data: {data.hex()}")
                
                # Print TX power if available
                if adv.tx_power is not None:
                    print(f"   TX Power: {adv.tx_power} dBm")
                
                print("-" * 60)
    
    scanner = BleakScanner(detection_callback)
    await scanner.start()
    await asyncio.sleep(timeout)
    await scanner.stop()
    
    print(f"\n{'=' * 60}")
    print(f"Scan complete. Found {len(found_devices)} NEEY balancer(s).")
    
    if found_devices:
        print("\nSummary of found devices:")
        for i, device in enumerate(found_devices, 1):
            print(f"  {i}. {device.name} ({device.address})")
        print(f"\nUse MAC address in your Python script:")
        print(f"  BALANCER_MAC = \"{found_devices[0].address}\"")
    else:
        print("\nNo NEEY balancers found. Troubleshooting tips:")
        print("  1. Ensure balancer is powered on (LED should be on)")
        print("  2. Ensure balancer is in pairing/advertising mode")
        print("  3. Check if Bluetooth is enabled: sudo systemctl status bluetooth")
        print("  4. Try moving closer to the Raspberry Pi")
        print("  5. Check if the app can see it - if not, balancer may be off/faulty")
    
    return found_devices


async def detailed_scan(mac_address: str, timeout: float = 10.0):
    """Perform detailed scan of a specific device"""
    print(f"\nDetailed scan of {mac_address}...")
    
    device = await BleakScanner.find_device_by_address(mac_address, timeout=timeout)
    
    if device is None:
        print(f"Device {mac_address} not found")
        return
    
    print(f"Found: {device.name} ({device.address})")
    print(f"Details: {device.details}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scan for NEEY Battery Balancers")
    parser.add_argument("--timeout", "-t", type=float, default=15.0, 
                       help="Scan duration in seconds (default: 15)")
    parser.add_argument("--mac", "-m", type=str, 
                       help="Get detailed info for specific MAC address")
    
    args = parser.parse_args()
    
    try:
        if args.mac:
            asyncio.run(detailed_scan(args.mac, args.timeout))
        else:
            asyncio.run(scan_neey(args.timeout))
    except KeyboardInterrupt:
        print("\n\nScan interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)
		