import traceback

from modules import amqp
from modules import db
import logging
import time


def result_processing(lst, settings, evt_queue_new_item, evt_upd_scheduler, call_counter, evt_error):
    dbh = db.connect()

    def cb_amqp_message(channel, method_frame, header_frame, body):
        process_amqp_message(dbh, lst, settings, evt_queue_new_item, evt_upd_scheduler, call_counter, evt_error, body)
        print('header_frame', header_frame)
        channel.basic_ack(method_frame.delivery_tag)

    if dbh is None:
        logging.error("result_processing: DB connection error. Terminating")
        print("result_processing: DB connection error. Terminating")
        return None

    with amqp.connect() as channel:
        channel.basic_consume('pbx_mfc_prequeue_result', cb_amqp_message)
        try:
            channel.start_consuming()
        except Exception as e:
            logging.error("result_processing: AMQP Exception %s\n%s", str(e), traceback.print_exc())
            print("result_processing: AMQP Exception %s\n%s", str(e), traceback.print_exc())
            return None


def process_amqp_message(dbh, lst, settings, evt_queue_new_item, evt_upd_scheduler, call_counter, evt_error, body):
    # raise Exception("Invalid message received")
    print(body)
