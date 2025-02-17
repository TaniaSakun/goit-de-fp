import logging
from kafka.admin import KafkaAdminClient, NewTopic
from configs import config

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_kafka_admin():
    """Initialize and return Kafka Admin Client."""
    try:
        return KafkaAdminClient(
            bootstrap_servers=config['bootstrap_servers'],
            security_protocol=config['security_protocol'],
            sasl_mechanism=config['sasl_mechanism'],
            sasl_plain_username=config['username'],
            sasl_plain_password=config['password']
        )
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def create_topic(admin_client, topic_name, num_partitions=2, replication_factor=1):
    """Create a Kafka topic with given specifications."""
    try:
        new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        logging.info(f'Topic "{topic_name}" created successfully.')
    except Exception as e:
        logging.error(f'Error creating topic "{topic_name}": {e}')

def main():
    """Main function to create Kafka topics."""
    my_name = config["my_name"]
    topics = [
        f"{my_name}_athlete_event_results",
        f"{my_name}_enriched_athlete_avg"
    ]
    
    admin_client = create_kafka_admin()
    if not admin_client:
        logging.error("Exiting due to Kafka connection failure.")
        return
    
    for topic in topics:
        create_topic(admin_client, topic)

    # Close connection
    admin_client.close()
    logging.info("Kafka admin client closed.")

if __name__ == "__main__":
    main()