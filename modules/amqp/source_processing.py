from modules import amqp
from modules.helpers import phone_format
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import time
import json
import uuid
import pytz

# TimeZone of project
EKT = pytz.timezone('Asia/Yekaterinburg')


def source_processing(lst, settings, evt_queue_new_item, evt_upd_scheduler, evt_error):
    print(settings)
    task_active = False
    scheduler = BackgroundScheduler(timezone=EKT)
    schedule_load_data(scheduler, lst, settings, evt_queue_new_item, task_active)
    scheduler.start()

    try:
        while True:
            evt_upd_scheduler.wait()
            schedule_load_data(scheduler, lst, settings, evt_queue_new_item, task_active)
            evt_upd_scheduler.clear()
    except KeyboardInterrupt:
        print('terminating.')

    print('SourceProcessing::Exiting')
    """
    print(settings.get(1, {}).get('download_time', '10:00'))
    get_from_file(lst, evt_queue_new_item)
    time.sleep(10)
    print(datetime.datetime.now().strftime("%H:%M:%S"), ': Process terminated')
    """


def schedule_load_data(scheduler, lst, settings, evt_queue_new_item, task_active):
    print('Jobs before adding')
    print(scheduler.get_jobs())

    job_id = scheduler.get_job(job_id='load_data')
    print(scheduler.get_job(job_id='load_data'))

    start_time_str = settings['settings']['download_time']
    start_time_lst = start_time_str.split(':')
    start_time_hour = int(start_time_lst[0])
    start_time_minute = int(start_time_lst[1])

    print(f"start time: {start_time_hour}:{start_time_minute}")
    func_args = {
        "lst": lst,
        "evt": evt_queue_new_item,
        "active": task_active
    }

    if job_id:
        print('ReSchedule job')
        scheduler.reschedule_job(
            job_id='load_data',
            trigger='cron',
            hour=start_time_hour,
            minute=start_time_minute
        )
    else:
        print("Add new job")
        scheduler.add_job(
            func=get_dummy,
            trigger='cron',
            hour=start_time_hour,
            minute=start_time_minute,
            id="load_data",
            kwargs=func_args
        )

    print('Jobs after change')
    print(scheduler.get_jobs())


def get_dummy(lst, evt, active):
    if active:
        print("GET::Task is active. Exiting")
        return None

    print("GET::Task NOT active. Continue")
    print(datetime.datetime.now().strftime("%H:%M:%S"), "GET::Task started. Waiting 60 sec")
    active = True
    time.sleep(300)
    print(datetime.datetime.now().strftime("%H:%M:%S"), "GET::Task finished. Exit")
    active = False
    return None

def get_from_file(lst, evt):
    print(datetime.datetime.now().strftime("%H:%M:%S"), ': Loading data from file')
    src_file_name = './src/api_data.json'
    with open(src_file_name, 'r', encoding="utf-8") as src_file:
        data = json.load(src_file)

    cnt = 0
    for idx in data:
        cnt += 1
        row_uuid = uuid.uuid4()
        item = {
            "id": None,
            "uuid": row_uuid,
            "mfc_number": idx["number"],
            "mfc_date": idx["dateVisit"],
            "mfc_time_from": idx["dateVisitTimeBeg"],
            "mfc_time_before": idx["dateVisitTimeEnd"],
            "mfc_phone": idx["phone"],
            "phone": phone_format.e164(idx["phone"]),
            "last_call_date": None,
            "last_call_disposition": None,
            "retry_count": 0,
            "call_result": None
        }

        lst[row_uuid] = item

    if cnt > 0:
        evt.set()


def xxx_fuck():
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
