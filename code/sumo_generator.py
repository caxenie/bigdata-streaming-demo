import json
import os
import sys
from typing import List, Dict, Any
import traci
import traci.constants as tc
from kafka import KafkaProducer

os.environ["SUMO_HOME"] = "C:/Program Files (x86)/Eclipse/Sumo/"

if 'SUMO_HOME' in os.environ:
    tools = os.path.join(os.environ['SUMO_HOME'], 'tools')
    sys.path.append(tools)
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")

# Starten Sie den SUMO Traffic Simulator
def start_sumo(sumo_config: str):
    sumo_cmd = ["sumo-gui", "-c", sumo_config, "--start", "--step-length", "1"]
    traci.start(sumo_cmd)

# Wählen Sie die Art der Daten, die vom Simulator gesammelt werden sollen.
def create_edge_subscriptions() -> List[str]:
    edge_ids = traci.edge.getIDList()
    for edge_id in edge_ids:
        traci.edge.subscribe(
            edge_id,
            [tc.LAST_STEP_VEHICLE_ID_LIST, tc.LAST_STEP_VEHICLE_NUMBER]
        )
    return edge_ids

# Funktion zum Schreiben der Simulator-Daten in den Kafka-Bus
def publish_message(
        producer: KafkaProducer, topic: str, value: Dict[str, Any]
):
    try:
        producer.send(topic, value=value)
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

# Erstellen Sie einen Datenproduzenten für den Simulator
def create_kafka_producer() -> KafkaProducer:
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            api_version=(0, 10),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer

# Run Simulation
def run_simulation(
        steps: int, edges: List[str], producer: KafkaProducer
) -> None:
    for step in range(steps):
        traci.simulationStep()
        for edge in edges:
            # Wähle das Kafka-Topic, in das die Daten geschrieben werden
            result = traci.edge.getSubscriptionResults(edge)
            for veh_id in result[tc.LAST_STEP_VEHICLE_ID_LIST]:
                publish_message(
                    producer=producer,
                    topic="source_topic",
                    value={
                        "step": float(step),
                        "edge_id": edge,
                        "vehicle_id": veh_id
                    }
                )
                print(f"{step};{edge};{veh_id}")
            vehicle_number = result[tc.LAST_STEP_VEHICLE_NUMBER]
            # Prüfen, ob neue Fahrzeuge passieren
            if vehicle_number > 0:
                publish_message(
                    producer=producer,
                    topic="source_num",
                    value={
                        "step": float(step),
                        "edge_id": edge,
                        "vehicle_num": vehicle_number
                    }
                )
                print(f"{step};{edge};{vehicle_number}")
            else:
                # Berücksichtige nur den Rand (Heydeck - Östliche Ringstraße)
                if edge == '32009826#1':
                    publish_message(
                        producer=producer,
                        topic="source_num",
                        value={
                            "step": float(step),
                            "edge_id": edge,
                            "vehicle_num": vehicle_number
                        }
                    )
                    print(f"{step};{edge};{vehicle_number}")
        producer.flush()


def close_sumo() -> None:
    traci.close()

# Funktion zum Starten der Datenstromerzeugung
def sumo_generator(sumo_config: str, steps: int) -> None:
    producer = create_kafka_producer()
    start_sumo(sumo_config)
    edge_ids = create_edge_subscriptions()
    try:
        run_simulation(steps, edge_ids, producer)
    except KeyboardInterrupt:
        pass
    finally:
        close_sumo()


if __name__ == "__main__":
    base_path = os.path.dirname(os.path.realpath(__file__))
    SUMO_CFG = f"D:/dev/sumo-flink-example-master/InTAS/scenario/InTAS_full_poly.sumocfg"
    sumo_generator(sumo_config=SUMO_CFG, steps=10000)
