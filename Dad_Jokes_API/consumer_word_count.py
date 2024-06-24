from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

KAFKA_TOPIC = 'jokes'
KAFKA_BOOTSTRAP_SERVERS = ':55134'


def word_count(message):
    jokes_log = message.value().decode('utf-8')
    wordcount= len(jokes_log.split(' '))
    return wordcount


def main():
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'jokes_word_count'}

    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])

        count = 0
        total_wrdcount = 0
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
                count+=1
                print(f"Number of jokes_received: {count}")
                wrdcount = word_count(msg)
                print(f'Word_count for present joke: {wrdcount}')
                total_wrdcount += wrdcount
                print(f'Cumulative word count in all jokes: {total_wrdcount}')
                if count % 5 == 0:
                    avg_wrd_count5 = round((total_wrdcount/count), 2)
                else: continue
                print(f'Average word count after every 5 jokes: {avg_wrd_count5}')


    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n') ## %% - system log

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == '__main__':
    main()