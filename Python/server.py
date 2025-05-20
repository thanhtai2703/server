
import logging
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from flask import Flask, jsonify, request
from flask_cors import CORS
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Configuration
class Config:
    # MQTT Configuration (EMQX Cloud)
    MQTT_BROKER = os.getenv("MQTT_BROKER", "b5619a98.ala.asia-southeast1.emqxsl.com")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
    MQTT_USERNAME = os.getenv("MQTT_USERNAME", "thanhtai")
    MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "thanhtai")
    MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", f"python-mqtt-{int(time.time())}")
    # Updated topic
    MQTT_TOPICS = os.getenv("MQTT_TOPICS", "sensors/all/room1,sensors/all/garage").split(",")
    MQTT_QOS = int(os.getenv("MQTT_QOS", 0))
    MQTT_CA_CERT = os.getenv("MQTT_CA_CERT", "C:\\Users\\taith\\Downloads\\emqxsl-ca.crt")

    # InfluxDB Configuration
    INFLUXDB_URL = os.getenv("INFLUXDB_URL", "https://us-east-1-1.aws.cloud2.influxdata.com")
    INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN",
                               "_8nfCZ3FNhXZKoexUIQVQG10wVg7Hkmq6ZbAEEE2-NMwHfC-bX3xofJEaySvgAF5mEr30Ba_TqLaKZQUcYs78Q==")
    INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "Embeded")
    INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "DHT11")


class EMQXToInfluxDB:
    def __init__(self, config: Config):
        self.config = config
        self.mqtt_client = None
        self.influxdb_client = None
        self.write_api = None
        self.query_api = None
        self.connected_to_mqtt = False
        self.connected_to_influxdb = False

    def setup_mqtt(self):
        try:
            self.mqtt_client = mqtt.Client(client_id=self.config.MQTT_CLIENT_ID)
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect = self.on_disconnect

            # Enable TLS/SSL with CA certificate
            self.mqtt_client.tls_set(ca_certs=self.config.MQTT_CA_CERT)
            self.mqtt_client.tls_insecure_set(False)

            if self.config.MQTT_USERNAME and self.config.MQTT_PASSWORD:
                self.mqtt_client.username_pw_set(self.config.MQTT_USERNAME, self.config.MQTT_PASSWORD)

            logger.info(f"Connecting to MQTT broker at {self.config.MQTT_BROKER}:{self.config.MQTT_PORT}")
            self.mqtt_client.connect(self.config.MQTT_BROKER, self.config.MQTT_PORT, 60)
            return True
        except Exception as e:
            logger.error(f"Failed to set up MQTT client: {e}")
            return False

    def setup_influxdb(self):
        try:
            self.influxdb_client = InfluxDBClient(
                url=self.config.INFLUXDB_URL,
                token=self.config.INFLUXDB_TOKEN,
                org=self.config.INFLUXDB_ORG
            )
            self.write_api = self.influxdb_client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.influxdb_client.query_api()
            logger.info("Successfully connected to InfluxDB Cloud")
            self.connected_to_influxdb = True
            return True
        except Exception as e:
            logger.error(f"Failed to set up InfluxDB client: {e}")
            return False

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info(f"Connected to MQTT broker successfully at {self.config.MQTT_BROKER}:{self.config.MQTT_PORT}")
            self.connected_to_mqtt = True
            # Subscribe to all configured topics
            for topic in self.config.MQTT_TOPICS:
                client.subscribe(topic, self.config.MQTT_QOS)
                logger.info(f"Subscribed to topic: {topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker with code {rc}")

    def on_disconnect(self, client, userdata, rc):
        logger.warning(f"Disconnected from MQTT broker with code {rc}")
        self.connected_to_mqtt = False

    def on_message(self, client, userdata, msg):
        try:
            logger.debug(f"Received message on topic {msg.topic}: {msg.payload}")
            # Parse the JSON payload
            payload_str = msg.payload.decode('utf-8')
            payload_data = json.loads(payload_str)

            # Extract data from the JSON payload
            temperature = payload_data.get('temperature')
            humidity = payload_data.get('humidity')
            unit = payload_data.get('unit', 'celsius')
            timestamp = payload_data.get('timestamp')

            # Determine location from topic
            location = "unknown"
            if "room1" in msg.topic:
                location = "MainHome"
            elif "garage" in msg.topic:
                location = "Garage"

            logger.info(f"Processed data from {location}: Temperature={temperature}°{unit}, Humidity={humidity}%")

            # Write temperature data
            if temperature is not None:
                # Convert temperature to float to ensure consistent data type
                temp_data = {"value": float(temperature)}
                self.write_to_influxdb("temperature", {"device": "ESP32", "unit": unit, "location": location},
                                       temp_data)

            # Write humidity data
            if humidity is not None:
                # Convert humidity to float to ensure consistent data type
                humidity_data = {"value": float(humidity)}
                self.write_to_influxdb("humidity", {"device": "ESP32", "location": location}, humidity_data)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Payload was: {msg.payload}")

    def write_to_influxdb(self, measurement: str, tags: Dict[str, str], fields: Dict[str, Any]):
        if not self.connected_to_influxdb:
            logger.warning("Not connected to InfluxDB, skipping write")
            return
        try:
            point = Point(measurement)
            for tag_key, tag_value in tags.items():
                point = point.tag(tag_key, tag_value)
            for field_key, field_value in fields.items():
                if isinstance(field_value, (int, float)):
                    # Convert all numeric values to float to ensure consistent data types
                    point = point.field(field_key, float(field_value))
                else:
                    point = point.field(field_key, str(field_value))
            point = point.time(datetime.utcnow(), WritePrecision.NS)
            self.write_api.write(bucket=self.config.INFLUXDB_BUCKET, org=self.config.INFLUXDB_ORG, record=point)
            logger.debug(f"Successfully wrote data to InfluxDB: {measurement}")
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")

    def get_historical_data(self, measurement: str, hours: int = 24, location: str = None) -> List[Dict[str, Any]]:
        """
        Query InfluxDB for historical data of a specific measurement

        Args:
            measurement: The measurement to query (temperature or humidity)
            hours: Number of hours to look back
            location: Optional location filter (MainHome or Garage)

        Returns:
            List of data points with time and value
        """
        if not self.connected_to_influxdb:
            logger.warning("Not connected to InfluxDB, cannot query data")
            return []

        try:
            # Calculate the time range
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)

            # Build the Flux query with optional location filter
            location_filter = ""
            if location:
                location_filter = f'|> filter(fn: (r) => r["location"] == "{location}")'

            query = f'''
            from(bucket: "{self.config.INFLUXDB_BUCKET}")
              |> range(start: {start_time.isoformat()}Z, stop: {end_time.isoformat()}Z)
              |> filter(fn: (r) => r["_measurement"] == "{measurement}")
              |> filter(fn: (r) => r["_field"] == "value")
              {location_filter}
              |> aggregateWindow(every: 2m, fn: mean, createEmpty: false)
              |> yield(name: "mean")
            '''

            # Execute the query
            result = self.query_api.query(query=query, org=self.config.INFLUXDB_ORG)

            # Process the results
            data_points = []
            for table in result:
                for record in table.records:
                    # Include location in the response if available
                    point = {
                        "time": record.get_time().isoformat(),
                        "value": record.get_value()
                    }

                    # Add location if present in the record
                    record_location = record.values.get("location")
                    if record_location:
                        point["location"] = record_location

                    data_points.append(point)

            return data_points
        except Exception as e:
            logger.error(f"Error querying InfluxDB for {measurement} history: {e}")
            return []

    def start_api_server(self, host='0.0.0.0', port=5000):
        """
        Start a Flask API server to serve historical data
        """
        app = Flask(__name__)
        CORS(app)  # Enable CORS for all routes

        @app.route('/api/temperature/history', methods=['GET'])
        def temperature_history():
            hours = request.args.get('hours', default=24, type=int)
            location = request.args.get('location', default=None, type=str)
            data = self.get_historical_data('temperature', hours, location)
            return jsonify({'data': data})

        @app.route('/api/humidity/history', methods=['GET'])
        def humidity_history():
            hours = request.args.get('hours', default=24, type=int)
            location = request.args.get('location', default=None, type=str)
            data = self.get_historical_data('humidity', hours, location)
            return jsonify({'data': data})

        @app.route('/api/locations', methods=['GET'])
        def get_locations():
            return jsonify({
                'locations': ['MainHome', 'Garage']
            })

        @app.route('/api/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'ok',
                'mqtt_connected': self.connected_to_mqtt,
                'influxdb_connected': self.connected_to_influxdb
            })

        logger.info(f"Starting API server on {host}:{port}")
        app.run(host=host, port=port, debug=False, threaded=True)

    def run(self):
        if not self.setup_mqtt():
            logger.error("Failed to set up MQTT client, exiting")
            return
        if not self.setup_influxdb():
            logger.error("Failed to set up InfluxDB client, exiting")
            return

        # Start MQTT client in a separate thread
        self.mqtt_client.loop_start()

        # Start API server in a separate thread
        api_thread = threading.Thread(target=self.start_api_server)
        api_thread.daemon = True
        api_thread.start()

        try:
            while True:
                time.sleep(10)  # Keep the script running
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
        finally:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            if self.influxdb_client:
                self.influxdb_client.close()
            logger.info("Shutdown complete")


def main():
    config = Config()
    emqx_to_influxdb = EMQXToInfluxDB(config)
    emqx_to_influxdb.run()


if __name__ == "__main__":
    main()