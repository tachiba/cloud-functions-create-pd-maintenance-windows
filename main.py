import base64
import requests
import json
import dateutil.parser
from datetime import datetime, timedelta


def run(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message_json = json.loads(pubsub_message)

    print(pubsub_message_json)
    print(type(pubsub_message_json))

    create_maintenance_windows(**pubsub_message_json)


def create_maintenance_windows(api_key, email, start_date, duration, frequency, repeat, description, services):
    headers = {
        'Authorization': 'Token token=' + api_key,
        'Content-type': 'application/json',
        'Accept': 'application/vnd.pagerduty+json;version=2',
        'From': email
    }

    now = datetime.now()
    start = dateutil.parser.parse(start_date).replace(year=now.year, month=now.month, day=now.day)
    end = start + timedelta(minutes=int(duration))
    for i in range(0, int(repeat), 1):
        payload = {
            'maintenance_window': {
                'type': 'maintenance_window',
                'start_time': start.isoformat(),
                'end_time': end.isoformat(),
                'description': description,
                'services': services
            }
        }
        print(f'Creating a {duration} minute maintenance window starting at {start}')
        r = requests.post('https://api.pagerduty.com/maintenance_windows', headers=headers, data=json.dumps(payload))
        if r.status_code == 201:
            id = r.json()['maintenance_window']['id']
            print(f'Maintenance window with ID {id} was successfully created')
        else:
            print(f'Error: maintenance window creation failed!\nStatus code: {r.status_code}\nResponse: {r.text}\nExiting...')
        start = start + timedelta(hours=int(frequency))
        end = end + timedelta(hours=int(frequency))
