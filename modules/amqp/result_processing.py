from modules import amqp
from modules import db
import logging
import time


def result_processing(lst, evt_queue_new_item, evt_upd_scheduler, evt_error):
    dbh = db.connect()

    def cb_message(ch, method, properties, body):
        result = amqp.get_message(ch, method, properties, body)
        print(result)

    with amqp.connect() as channel:
        try:
            channel.queue_bind(
                queue="pbx_mfc_prequeue_result",
                exchange="pbx_mfc",
                routing_key="pbx_mfc_prequeue_result"
            )

            channel.basic_consume(
                queue="pbx_mfc_prequeue_result",
                auto_ack=True,
                on_message_callback=cb_message
            )

            print(" [*] Waiting for RESULT messages. To exit press CTRL+C")
            channel.start_consuming()

        except Exception as e:
            logging.error("result_processing: %s", str(e))
            print(f"result_processing.error: str(e)", )
