from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':55134'

def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'joke_logger'}

    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(msg.value().decode())

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n') ## %% - system log

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == '__main__':
    main()
