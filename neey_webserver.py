#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

# Global storage for latest data
latest_data = None
data_lock = threading.Lock()
last_update = 0

# MQTT Settings
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "NEEY/data"

class MQTTHandler:
    def __init__(self):
        # MQTT v2.0 API - use CallbackAPIVersion.VERSION2
        self.client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
    def on_connect(self, client, userdata, connect_flags, reason_code, properties=None):
        if reason_code == 0:
            print(f"Connected to MQTT broker successfully")
            client.subscribe(MQTT_TOPIC)
            print(f"Subscribed to {MQTT_TOPIC}")
        else:
            print(f"Failed to connect, reason code: {reason_code}")
        
    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        print(f"Disconnected from MQTT broker with reason code: {reason_code}")
        # Attempt to reconnect
        while True:
            try:
                print("Attempting to reconnect...")
                self.client.reconnect()
                break
            except Exception as e:
                print(f"Reconnect failed: {e}")
                time.sleep(5)
                
    def on_message(self, client, userdata, msg):
        global latest_data, last_update
        
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8').strip()
            
            # Only process NEEY/data topic with JSON payload
            if topic == "NEEY/data":
                data = json.loads(payload)
                with data_lock:
                    latest_data = data
                    last_update = time.time()
                print(f"Received complete data packet, cells: {len(data.get('cells', []))}")
                    
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def start(self):
        while True:
            try:
                self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
                self.client.loop_forever()
            except Exception as e:
                print(f"MQTT connection failed: {e}")
                time.sleep(5)

class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
        
    def do_GET(self):
        if self.path == '/data':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            
            with data_lock:
                if latest_data:
                    response_data = latest_data.copy()
                    response_data['stale'] = (time.time() - last_update) > 30
                    response_data['last_seen'] = int(time.time() - last_update)
                    response = json.dumps(response_data).encode()
                else:
                    # Return empty structure if no data yet
                    response = json.dumps({
                        "timestamp": "",
                        "device": {},
                        "battery": {},
                        "cells": [],
                        "stale": True,
                        "last_seen": 0
                    }).encode()
                    
            self.wfile.write(response)
            
        elif self.path == '/' or self.path == '/index.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(HTML_CONTENT.encode())
            
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

HTML_CONTENT = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NEEY BMS Monitor - GW-24S4EB</title>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            min-height: 100vh;
            color: #fff;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        header {
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }

        .timestamp {
            color: #a0c4ff;
            font-size: 0.9em;
        }

        .connection-status {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-left: 10px;
            background: #ef4444;
            transition: background 0.3s;
        }

        .connection-status.connected {
            background: #22c55e;
            box-shadow: 0 0 10px #22c55e;
        }

        .stale-warning {
            background: rgba(245, 158, 11, 0.2);
            border: 1px solid #f59e0b;
            color: #fcd34d;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            display: none;
            text-align: center;
        }

        .waiting-message {
            text-align: center;
            padding: 50px;
            font-size: 1.2em;
            color: #94a3b8;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }

        .card {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            padding: 25px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }

        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }

        .card h2 {
            font-size: 1.3em;
            margin-bottom: 15px;
            color: #ffd700;
            border-bottom: 2px solid rgba(255, 215, 0, 0.3);
            padding-bottom: 10px;
        }

        .info-row {
            display: flex;
            justify-content: space-between;
            margin: 12px 0;
            padding: 8px 0;
            border-bottom: 1px solid rgba(255,255,255,0.1);
        }

        .info-row:last-child {
            border-bottom: none;
        }

        .label {
            color: #b0c4de;
            font-weight: 500;
        }

        .value {
            font-weight: bold;
            color: #fff;
        }

        .voltage-high { color: #4ade80; }
        .voltage-low { color: #f87171; }
        .voltage-normal { color: #fbbf24; }

        .status-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
        }

        .status-active {
            background: #22c55e;
            color: white;
        }

        .status-inactive {
            background: #ef4444;
            color: white;
        }

        .cells-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }

        .cell-card {
            background: rgba(255, 255, 255, 0.05);
            border-radius: 10px;
            padding: 15px;
            border: 1px solid rgba(255,255,255,0.1);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }

        .cell-card:hover {
            background: rgba(255, 255, 255, 0.15);
            transform: scale(1.02);
        }

        .cell-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 4px;
            height: 100%;
            background: linear-gradient(to bottom, #f59e0b, #10b981);
        }

        .cell-number {
            font-size: 0.9em;
            color: #94a3b8;
            margin-bottom: 5px;
        }

        .cell-voltage {
            font-size: 1.4em;
            font-weight: bold;
            margin-bottom: 5px;
        }

        .cell-resistance {
            font-size: 0.85em;
            color: #cbd5e1;
        }

        .progress-bar {
            width: 100%;
            height: 6px;
            background: rgba(255,255,255,0.1);
            border-radius: 3px;
            margin-top: 8px;
            overflow: hidden;
        }

        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #f59e0b, #10b981);
            border-radius: 3px;
            transition: width 0.5s ease;
        }

        .refresh-indicator {
            position: fixed;
            top: 20px;
            right: 20px;
            background: rgba(0,0,0,0.5);
            padding: 10px 20px;
            border-radius: 25px;
            font-size: 0.9em;
            display: none;
            align-items: center;
            gap: 10px;
            backdrop-filter: blur(10px);
        }

        .pulse {
            width: 10px;
            height: 10px;
            background: #3b82f6;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(59, 130, 246, 0.7); }
            70% { transform: scale(1); box-shadow: 0 0 0 10px rgba(59, 130, 246, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(59, 130, 246, 0); }
        }

        .stats-summary {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin-bottom: 20px;
        }

        .stat-box {
            background: rgba(255,255,255,0.05);
            padding: 15px;
            border-radius: 10px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
        }

        .stat-value {
            font-size: 1.8em;
            font-weight: bold;
            color: #ffd700;
        }

        .stat-label {
            font-size: 0.8em;
            color: #94a3b8;
            margin-top: 5px;
        }

        @media (max-width: 768px) {
            .grid {
                grid-template-columns: 1fr;
            }
            .cells-grid {
                grid-template-columns: 1fr;
            }
            .stats-summary {
                grid-template-columns: repeat(2, 1fr);
            }
            h1 {
                font-size: 1.8em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔋 NEEY BMS Monitor <span class="connection-status" id="conn-status"></span></h1>
            <div class="timestamp" id="timestamp">Waiting for MQTT data...</div>
        </header>

        <div class="stale-warning" id="stale-warning">
            ⚠️ Data is stale (last update: <span id="stale-secs">0</span>s ago)
        </div>

        <div class="waiting-message" id="waiting">
            <div>⏳ Waiting for data from MQTT...</div>
            <div style="font-size: 0.8em; margin-top: 10px;">Ensure NEEY BMS is publishing to NEEY/data topic</div>
        </div>
        
        <div class="stats-summary" id="stats-summary" style="display: none;">
            <div class="stat-box">
                <div class="stat-value" id="stat-total-v">--</div>
                <div class="stat-label">Total Voltage</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" id="stat-avg-v">--</div>
                <div class="stat-label">Avg Cell V</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" id="stat-delta">--</div>
                <div class="stat-label">Delta</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" id="stat-temp">--</div>
                <div class="stat-label">Temperature</div>
            </div>
        </div>

        <div id="content" style="display: none;">
            <div class="grid">
                <div class="card">
                    <h2>📟 Device Information</h2>
                    <div class="info-row">
                        <span class="label">Model</span>
                        <span class="value" id="device-model">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Hardware Version</span>
                        <span class="value" id="device-hw">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Software Version</span>
                        <span class="value" id="device-sw">-</span>
                    </div>
                </div>

                <div class="card">
                    <h2>⚡ Battery Overview</h2>
                    <div class="info-row">
                        <span class="label">Total Voltage</span>
                        <span class="value voltage-high" id="total-voltage">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Average Cell Voltage</span>
                        <span class="value" id="avg-voltage">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Cell Count</span>
                        <span class="value" id="cell-count">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Delta Voltage</span>
                        <span class="value" id="delta-voltage">-</span>
                    </div>
                </div>

                <div class="card">
                    <h2>🌡️ Temperature & Status</h2>
                    <div class="info-row">
                        <span class="label">Temperature 1</span>
                        <span class="value" id="temp1">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Temperature 2</span>
                        <span class="value" id="temp2">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Balancing</span>
                        <span class="status-badge" id="balancing">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Status Code</span>
                        <span class="value" id="status-code">-</span>
                    </div>
                </div>

                <div class="card">
                    <h2>📊 Voltage Extremes</h2>
                    <div class="info-row">
                        <span class="label">Max Cell Voltage</span>
                        <span class="value voltage-high" id="max-voltage">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Min Cell Voltage</span>
                        <span class="value voltage-low" id="min-voltage">-</span>
                    </div>
                    <div class="info-row">
                        <span class="label">Voltage Spread</span>
                        <span class="value" id="voltage-spread">-</span>
                    </div>
                </div>
            </div>

            <div class="card" style="margin-top: 20px;">
                <h2>🔋 Individual Cell Voltages</h2>
                <div class="cells-grid" id="cells-container">
                </div>
            </div>
        </div>

        <div class="refresh-indicator" id="refresh-indicator">
            <div class="pulse"></div>
            <span>Live Updates</span>
        </div>
    </div>

    <script>
        $(document).ready(function() {
            let lastData = null;
            let updateInterval;
            
            function fetchData() {
                $.ajax({
                    url: '/data',
                    method: 'GET',
                    dataType: 'json',
                    timeout: 5000,
                    success: function(data) {
                        if (data && data.device && data.device.model) {
                            $('#waiting').hide();
                            $('#conn-status').addClass('connected');
                            
                            if (JSON.stringify(data) !== JSON.stringify(lastData)) {
                                updateDashboard(data);
                                lastData = data;
                            }
                            
                            if (data.stale) {
                                $('#stale-warning').show();
                                $('#stale-secs').text(data.last_seen);
                                $('#conn-status').removeClass('connected');
                            } else {
                                $('#stale-warning').hide();
                            }
                        } else {
                            $('#waiting').show();
                            $('#conn-status').removeClass('connected');
                        }
                    },
                    error: function(xhr, status, error) {
                        $('#conn-status').removeClass('connected');
                        console.log('Fetch error:', error);
                    }
                });
            }

            function updateDashboard(data) {
                $('#content').show();
                $('#stats-summary').show();
                $('#refresh-indicator').css('display', 'flex');

                const date = new Date(data.timestamp);
                $('#timestamp').text('Last Update: ' + date.toLocaleString());

                $('#stat-total-v').text(data.battery.total_voltage.toFixed(1) + 'V');
                $('#stat-avg-v').text(data.battery.average_cell_voltage.toFixed(2) + 'V');
                $('#stat-delta').text(data.battery.delta_voltage.toFixed(3) + 'V');
                $('#stat-temp').text(data.battery.temperature_1.toFixed(1) + '°C');

                $('#device-model').text(data.device.model);
                $('#device-hw').text(data.device.hw_version);
                $('#device-sw').text(data.device.sw_version);

                $('#total-voltage').text(data.battery.total_voltage.toFixed(3) + ' V');
                $('#avg-voltage').text(data.battery.average_cell_voltage.toFixed(3) + ' V');
                $('#cell-count').text(data.battery.cell_count);
                $('#delta-voltage').text(data.battery.delta_voltage.toFixed(3) + ' V');

                $('#temp1').text(data.battery.temperature_1.toFixed(1) + ' °C');
                $('#temp2').text(data.battery.temperature_2.toFixed(1) + ' °C');
                
                const balancing = data.battery.balancing;
                $('#balancing')
                    .text(balancing ? 'ACTIVE' : 'INACTIVE')
                    .removeClass('status-active status-inactive')
                    .addClass(balancing ? 'status-active' : 'status-inactive');
                
                $('#status-code').text(data.battery.status);

                $('#max-voltage').text(data.battery.max_cell_voltage.toFixed(3) + ' V');
                $('#min-voltage').text(data.battery.min_cell_voltage.toFixed(3) + ' V');
                $('#voltage-spread').text((data.battery.max_cell_voltage - data.battery.min_cell_voltage).toFixed(3) + ' V');

                renderCells(data.cells, data.battery.min_cell_voltage, data.battery.max_cell_voltage);
            }

            function renderCells(cells, minV, maxV) {
                const container = $('#cells-container');
                const range = maxV - minV || 0.001;
                
                container.empty();

                cells.forEach(function(cell) {
                    const voltage = cell.voltage;
                    const percentage = Math.min(100, Math.max(0, ((voltage - minV) / range) * 100));
                    const isHigh = Math.abs(voltage - maxV) < 0.001;
                    const isLow = Math.abs(voltage - minV) < 0.001;
                    
                    let voltageClass = 'voltage-normal';
                    if (isHigh) voltageClass = 'voltage-high';
                    if (isLow) voltageClass = 'voltage-low';

                    const cellHtml = `
                        <div class="cell-card">
                            <div class="cell-number">Cell #${cell.cell}</div>
                            <div class="cell-voltage ${voltageClass}">${voltage.toFixed(3)} V</div>
                            <div class="cell-resistance">${cell.resistance} mΩ</div>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: ${percentage}%"></div>
                            </div>
                        </div>
                    `;
                    container.append(cellHtml);
                });
            }

            fetchData();
            updateInterval = setInterval(fetchData, 5000);

            $(window).on('beforeunload', function() {
                clearInterval(updateInterval);
            });
        });
    </script>
</body>
</html>'''

def run_web_server():
    server = HTTPServer(('0.0.0.0', 2222), WebHandler)
    print(f"Web server started at http://127.0.0.1:2222")
    print("Waiting for MQTT data on topic: NEEY/data")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

if __name__ == "__main__":
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        print("Installing paho-mqtt...")
        import subprocess
        subprocess.check_call(['pip3', 'install', 'paho-mqtt'])
        import paho.mqtt.client as mqtt
    
    mqtt_handler = MQTTHandler()
    mqtt_thread = threading.Thread(target=mqtt_handler.start, daemon=True)
    mqtt_thread.start()
    
    time.sleep(1)
    run_web_server()
    