import random
import time
import json
import logging
import uuid
from datetime import datetime, timedelta
from threading import Event
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from azure.iot.device import IoTHubDeviceClient, Message
import requests


logging.basicConfig(level=logging.DEBUG)
# Prevent superfluous logging
logging.getLogger('azure.iot.device').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('paho').setLevel(logging.WARNING)


def _generate_ean():
    return 'EAN54' + ''.join([str(random.randint(0,9)) for i in range(16)])


def _get_rate():
    now = datetime.now()

    if now.hour >= 22 or now.hour < 7 or now.weekday() > 4:
        return 2
    else:
        return 1


def _has_anomaly():
    anomaly = random.choices([True, False],[2,98])[0]
    if anomaly:
        logging.warning('ANOMALY!')
    return anomaly


def _simulate_electricity(ean, previous_data=None):
    
    current_rate = _get_rate()
    
    data = {
        'ID': ean,
        'timestamp': datetime.now().isoformat(),
        'rate_1_total': 0.0,
        'rate_2_total': 0.0,
        'current_rate': current_rate,
        'L1_cons': random.uniform(0.02,5), #kW
        'L1_prod': 0.0,
        'L1_voltage': random.uniform(230.0, 240.0),
        'L1_current': random.uniform(0.5,16)
    }

    if previous_data is not None:
        data['rate_1_total'] = previous_data['rate_1_total']
        data['rate_2_total'] = previous_data['rate_2_total']

    # Update totals
    key = f'rate_{current_rate}_total'
    data[key] += data['L1_cons']

    return data


def _simulate_gas(ean, previous_data=None):
    
    data = {
        'ID': ean,
        'timestamp': datetime.now().isoformat(),
        'total_cons': 0.0,
        'current_cons': random.uniform(0.1,10.0)
    }

    if previous_data is not None:
        data['total_cons'] = previous_data['total_cons']

    data['total_cons'] += data['current_cons']

    return data


def _get_az_weather(lat, lon, subscription_key, client_id):
    url = f'https://eu.atlas.microsoft.com/weather/currentConditions/json?api-version=1.1&query={lat},{lon}&unit=metric&details=true&duration=0&language=en-GB&subscription-key={subscription_key}'
    r = requests.get(url, 
                 headers={'x-ms-client-id': client_id})
    try:
        r.raise_for_status()
    except Exception as e:
        logging.error(f'Failed to get weather data: {e!r}')
        return {}
    
    return r.json()['results'][0]


# TELEMETRY GENERATING FUNCTIONS

def generate_telemetry_utilty(stop_event, simulate_func, ean, conn_str, interval):

    client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    client.connect()
    data = None
    try:
        while not stop_event.is_set():
            next_exec = datetime.now() + timedelta(seconds=interval)
            data = simulate_func(ean, data)
            logging.debug(f'Message: {data}')
            if _has_anomaly():
                anomaly = copy(data)
                anomaly['ID'] = _generate_ean()
                if 'rate_1_total' in anomaly:
                    # Electricity
                    anomaly['rate_1_total'] *= -1
                    anomaly['L1_cons'] *= -1
                else:
                    anomaly['current_cons'] *= -1
                    anomaly['total_cons'] *= -1
                msg = Message(json.dumps(anomaly))
            else:
                msg = Message(json.dumps(data))

            msg.message_id = uuid.uuid4()
            msg.content_encoding = 'utf-8'
            msg.content_type = 'application/json'
            client.send_message(msg)

            while datetime.now() < next_exec and not stop_event.is_set():
                time.sleep(5)
    except Exception as e:
        logging.warning(f'Received exception {e!r}')
    finally:
        logging.debug('Cleaning up IoT client')
        client.disconnect()
        client.shutdown()


def generate_telemetry_weather(stop_event, conn_str, interval, **kwargs):
    client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    client.connect()
    try:
        while not stop_event.is_set():
            next_exec = datetime.now() + timedelta(seconds=interval) 
            data = _get_az_weather(**kwargs['coordinates'], 
                                   **kwargs['az_maps'])
            logging.debug(f'Message: {data}')
            # Generate anomaly?
            if _has_anomaly():
                anomaly = copy(data)
                anomaly['temperature']['value'] *= -3.5
                anomaly['uvIndex'] *= -1
                anomaly['relativeHumidity'] *= 3
                msg = Message(json.dumps(anomaly))
                msg.custom_properties['lat'] = 0.0
                msg.custom_properties['lon'] = 0.0
            else:
                msg = Message(json.dumps(data))
                msg.custom_properties['lat'] = kwargs['coordinates']['lat']
                msg.custom_properties['lon'] = kwargs['coordinates']['lon']

            msg.message_id = uuid.uuid4()
            msg.content_encoding = 'utf-8'
            msg.content_type = 'application/json'
            client.send_message(msg)
            
            while datetime.now() < next_exec and not stop_event.is_set():
                time.sleep(5)
    except Exception as e:
        logging.warning(f'Received exception {e!r}')
    finally:
        logging.debug('Cleaning up IoT client')
        client.disconnect()
        client.shutdown()


if __name__ == '__main__':

    with open('simul_config.json') as f:
        config = json.load(f)

    # Generate random EAN numbers
    config['gas']['ean'] = _generate_ean()
    config['electricity']['ean'] = _generate_ean()

    stop_event = Event()
    futures = []

    with ThreadPoolExecutor() as executor:
        logging.info('Setting up threads...')
        futures.append(executor.submit(generate_telemetry_utilty, 
                                    stop_event,
                                    _simulate_electricity, 
                                    **config['electricity']))

        futures.append(executor.submit(generate_telemetry_utilty, 
                                    stop_event,
                                    _simulate_gas, 
                                    **config['gas']))

        futures.append(executor.submit(generate_telemetry_weather,
                                    stop_event,
                                    **config['weather']))
        try:
            logging.info('Running...')
            while True:
                time.sleep(2)
        except KeyboardInterrupt:
            logging.info('Received interrupt, sending stop event to threads...')
            stop_event.set()
    logging.info('Threads stopped')