from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
# Connect to Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers='kafka:9092',
    client_id='topic-creator'
)

# Define the new topic
new_topic = NewTopic(
    name='listing-events',
    num_partitions=3,
    replication_factor=1
)

# Create the topic
try:
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
    print("Topic 'listing-events' created successfully!")
except TopicAlreadyExistsError:
    print('Topic Already Exists')
except Exception as e:
    print(f'Other Error {e}')

# Close the connection
admin_client.close()