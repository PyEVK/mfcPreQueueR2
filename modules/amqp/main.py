import yaml
import pika
import logging

AMQP_CONF_FILE_PATH = './conf/amqp.yaml'


def connect():
    with open(AMQP_CONF_FILE_PATH, 'r') as amqp_conf_file:
        amqp_conf = yaml.safe_load(amqp_conf_file)

    try:
        credentials = pika.PlainCredentials(username=amqp_conf["login"], password=amqp_conf["password"])
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=amqp_conf["host"],
                port=amqp_conf["port"],
                virtual_host=amqp_conf["virtual_host"],
                credentials=credentials
            )
        )

        channel = connection.channel()
        print(connection)
        print(channel)

        return channel
    except Exception as e:
        logging.error("AMQP Connect: %s", str(e))
        print("AMQP Connect: %s", str(e))
        return None
