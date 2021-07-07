#!/usr/bin/env python

from confluent_kafka import Consumer
import json
import ccloud_lib

import pymssql


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    # Connection SQL Server
    conn = pymssql.connect(
        host=r'clanfbicw01.sqm.local',
        user=r'sqmdom\USER',
        password='PASS',
        database='test'
    )
    cursor = conn.cursor()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                clirut = data['clirut']
                clidig = data['clidig']
                clinom = data['clinom']
                print("Inserted clirut: {},clidig: {},clinom: {}"
                      .format(str(clirut), str(clidig), str(clinom)))
                cursor.execute('INSERT INTO cliente VALUES (%s, %s, %s)', (clirut, clidig, clinom))
                conn.commit()

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        conn.close()
