from modules.helpers import phone_format
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import requests
import json
import uuid
import pytz

# TimeZone of project
EKT = pytz.timezone('Asia/Yekaterinburg')


def source_processing(lst, settings, evt_queue_new_item, evt_upd_scheduler, evt_error, lock):
    scheduler = BackgroundScheduler(timezone=EKT)
    schedule_load_data(scheduler, lst, settings, evt_queue_new_item, lock)
    scheduler.start()

    try:
        while True:
            evt_upd_scheduler.wait()
            schedule_load_data(scheduler, lst, settings, evt_queue_new_item, lock)
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


def schedule_load_data(scheduler, lst, settings, evt_queue_new_item, lock):
    job_id = scheduler.get_job(job_id='load_data')

    start_time_str = settings['settings']['download_time']
    start_time_lst = start_time_str.split(':')
    start_time_hour = int(start_time_lst[0])
    start_time_minute = int(start_time_lst[1])
    request_days = settings['settings']['download_days']

    func_args = {
        "lst": lst,
        "evt": evt_queue_new_item,
        "days": request_days,
        "lock": lock
    }

    if job_id:
        job_id.remove()
        # scheduler.remove_job(job_id=job_id)

    scheduler.add_job(
        func=get_from_api,
        trigger='cron',
        hour=start_time_hour,
        minute=start_time_minute,
        id="load_data",
        kwargs=func_args
    )


def get_from_file(lst, evt, lock):
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

        lock.acquire()
        lst[row_uuid] = item
        lock.release()

    if cnt > 0:
        evt.set()


def get_from_api(lst, evt, lock, days=2):
    print(f"Days: {days}")
    url = "http://172.29.125.19:8080/api/prequeue-phone-bot/prequeue.php"

    headers = {
        'Authorization': 'Bearer 6325293f6aa5826b6cea517986546bb7'
    }

    payload = {
        "number_day": days,
        "status": "NOT_ACTIVATED"
    }

    r = requests.get(url=url, headers=headers, params=payload)

    r_data = r.json()
    cnt = 0
    for idx in r_data:
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

        lock.acquire()
        lst[row_uuid] = item
        lock.release()

    if cnt > 0:
        evt.set()
