from modules import amqp
import time


def result_processing(lst, evt_queue_new_item, evt_upd_scheduler, evt_error):
    print("Result processing:::started")

    while True:
        evt_queue_new_item.wait()
        print("Result processing:::NewItemEvent")
        evt_queue_new_item.clear()
