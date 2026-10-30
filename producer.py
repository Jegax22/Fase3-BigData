"""
Productor Kafka: genera eventos simulados de viajes en taxi.

Requiere un broker Kafka disponible en localhost:9092 y el topic viajes_taxis.
Ejecución:
    python producer.py
Detención:
    Ctrl + C
"""
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "viajes_taxis"


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        retries=5,
    )

    print(f"Generador iniciado. Enviando eventos a {TOPIC} ({BOOTSTRAP_SERVERS})")

    try:
        while True:
            evento = {
                "id_viaje": random.randint(1000, 999999),
                "fecha": datetime.now(timezone.utc).isoformat(),
                "pasajeros": random.randint(1, 4),
                "distancia": round(random.uniform(1, 25), 2),
                "total": round(random.uniform(10, 100), 2),
            }

            future = producer.send(TOPIC, evento)
            metadata = future.get(timeout=10)
            print(
                f"Evento enviado a partición {metadata.partition}, "
                f"offset {metadata.offset}: {evento}"
            )
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nGenerador detenido por el usuario.")
    except KafkaError as exc:
        print(f"Error enviando evento a Kafka: {exc}")
        raise
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
