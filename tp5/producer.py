import json, random, time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['Electronique', 'Vetements', 'Maison', 'Livres', 'Sport']
event_types = ['click', 'add_to_cart', 'purchase']

print('Démarrage du producteur. Ctrl+C pour arrêter.')
try:
    while True:
        event = {
            'user_id'    : f'user_{random.randint(1, 100)}',
            'event_type' : random.choice(event_types),
            'category'   : random.choice(categories),
            'amount'     : round(random.uniform(5.0, 500.0), 2),
            'timestamp'  : int(time.time() * 1000)
        }
        producer.send('ecommerce-events', value=event)
        print(f' → {event}')
        time.sleep(0.5)
except KeyboardInterrupt:
    print('Arrêt du producteur.')
    producer.close()
