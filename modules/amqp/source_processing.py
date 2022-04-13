from modules import amqp
from modules import db
import schedule
import time


def source_processing(lst, evt_queue_new_item, evt_upd_scheduler, evt_error):
    dbh = db.connect()
    ch = amqp.connect()

    with ch:
        print("amqp channel", ch)

        try:
            # amqp_conn.queue_declare("pbx_mfc_prequeue_lst", auto_delete=False)

            """ """
            ch.queue_bind(
                queue="pbx_mfc_prequeue_lst",
                exchange="pbx_mfc",
                routing_key="pbx_mfc_prequeue_lst"
            )
            """ """
            args = dict({
                "dbh": dbh,
                "lst": lst
            })
            """ """
            ch.basic_consume(
                queue="pbx_mfc_prequeue_lst",
                auto_ack=True,
                on_message_callback=source_lst_processing,
                arguments={lst, dbh}
            )
            """ """

            print(" [*] Waiting for messages. To exit press CTRL+C")
            ch.start_consuming()

        except Exception as e:
            str_e = str(e)
            print(str_e)
            print("AMQP Exception", str_e, str_e.encode("UTF-8"))


def source_lst_processing(ch, method, properties, body, **kwargs):
    print("callback args", kwargs)


    """
    print("Source processing:::Started")
    cnt = 0
    while True:
        time.sleep(10)
        print("Source processing:::waked up after 10 sec")
        evt_queue_new_item.set()
        print("Source processing:::event cleared")
        cnt += 1
        if cnt > 3:
            raise Exception('Go away')
    """
