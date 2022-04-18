from apscheduler.schedulers.background import BackgroundScheduler
import datetime
import pytz
import threading

# TimeZone of project
EKT = pytz.timezone('Asia/Yekaterinburg')


def dialer(lst, settings, evt_queue_new_item, evt_upd_scheduler, call_counter, evt_error, lock):
    scheduler = BackgroundScheduler(timezone=EKT)
    schedule_dialer(scheduler, lst, settings, evt_queue_new_item, call_counter, lock)
    scheduler.start()



    """
        try:
            while True:
                print('Dialer::Waiting for schedule update events')
                evt_upd_scheduler.wait()
                schedule_dialer(scheduler, lst, settings, evt_queue_new_item, call_counter)
                evt_upd_scheduler.clear()
        except KeyboardInterrupt:
            print('terminating.')

        print('Dialer::Exiting')


    def schedule_dialer(scheduler, lst, settings, evt_queue_new_item, call_counter):
        print("Dialer: schedule_load_data")
    """


def schedule_dialer(scheduler, lst, settings, evt_queue_new_item, call_counter, lock):
    pass
