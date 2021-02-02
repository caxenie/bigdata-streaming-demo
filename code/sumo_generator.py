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

# Start the SUMO Simulator (Simulator for Urban Mobility)
def start_sumo(sumo_config: str):
    sumo_cmd = ["sumo-gui", "-c", sumo_config, "--start", "--step-length", "1"]
    traci.start(sumo_cmd)

# Define what information to request from the simulator
def create_edge_subscriptions() -> List[str]:
    edge_ids = traci.edge.getIDList()
    for edge_id in edge_ids:
        traci.edge.subscribe(
            edge_id,
            [tc.LAST_STEP_VEHICLE_ID_LIST, tc.LAST_STEP_VEHICLE_NUMBER]
        )
    return edge_ids

# Request the actual information from the simulator
def publish_message(
        producer: KafkaProducer, topic: str, value: Dict[str, Any]
):
    try:
        producer.send(topic, value=value)
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

# Create a Kafka producer to request data from the Simualtor (the Stream generator)
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

# Run the simulation and subscribe for updates on a certain edge / lane of the map
def run_simulation(
        steps: int, edges: List[str], producer: KafkaProducer
) -> None:
    for step in range(steps):
        traci.simulationStep()
        for edge in edges:
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
                if edge == '313576543#2': # Dachauerstrasse - Lothstrasse
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


# The stream generator
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
    SUMO_CFG = f"D:/dev/sumo-flink-example-master/SUMO_Map_Munich/bin/Hochschule_Munich_Viertel.sumo.cfg"
    sumo_generator(sumo_config=SUMO_CFG, steps=10000)
