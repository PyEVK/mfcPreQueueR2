import logging
import traceback
from logging.handlers import TimedRotatingFileHandler
import os
import pathlib
from datetime import datetime
import pytz
from modules import db
from modules import amqp
from modules import prequeue
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import signal
import psutil

# Set working directory to file location
os.chdir(pathlib.Path(__file__).parent.absolute())

# Set logging level for project
LOG_LEVEL = logging.DEBUG

# TimeZone of project
EKT = pytz.timezone('Asia/Yekaterinburg')


def main():
    log_init()

    # Prepare for multiprocessing
    print("Initialization")
    manager = multiprocessing.Manager()
    prequeue_lst = manager.dict()
    prequeue_settings = manager.dict()
    call_counter = manager.Value('i', 0)
    evt_queue_new_item = manager.Event()
    evt_upd_scheduler = manager.Event()
    evt_error = manager.Event()

    # Get init states
    dbh = db.connect()
    if dbh is None:
        print("DB connection error")
        exit(-1)

    prequeue.get_init_lst(dbh, prequeue_lst)
    prequeue.get_settings(dbh, prequeue_settings)

    """
    source_processing = multiprocessing.Process(
        target=amqp.source_processing,
        args=(
            prequeue_lst,
            prequeue_settings,
            evt_queue_new_item,
            evt_upd_scheduler,
            evt_error,
        )
    )

    result_processing = multiprocessing.Process(
        target=amqp.result_processing,
        args=(
            prequeue_lst,
            prequeue_settings,
            evt_queue_new_item,
            evt_upd_scheduler,
            evt_error,
        )
    )

    source_processing.start()
    result_processing.start()

    source_processing.join()
    result_processing.join()
    """

    """
    amqp_source_processing = multiprocessing.Process(
        target=amqp.source_processing,
        args=(
            prequeue_lst,
            evt_queue_new_item,
            evt_upd_scheduler,
            evt_error,
        )
    )

    amqp_result_processing = multiprocessing.Process(
        target=amqp.result_processing,
        args=(
            prequeue_lst,
            evt_queue_new_item,
            evt_upd_scheduler,
            evt_error,
        )
    )

    amqp_source_processing.start()
    amqp_result_processing.start()

    amqp_source_processing.join()
    amqp_result_processing.join()
    """

    """ """
    cf_features = []
    with ProcessPoolExecutor() as executor:
        # """
        cf_features.append(executor.submit(
            amqp.source_processing,
            prequeue_lst,
            prequeue_settings,
            evt_queue_new_item,
            evt_upd_scheduler,
            evt_error
        ))
        # """
        """
        cf_features.append(executor.submit(
            amqp.result_processing,
            prequeue_lst,
            prequeue_settings,
            evt_queue_new_item,
            evt_upd_scheduler,
            call_counter,
            evt_error
        ))
        """

        # Waiting for complete
        for future in as_completed(cf_features):
            proc = cf_features
            try:
                pass
            except Exception as exc:
                print('%r generated an exception: %s' % (proc, exc))
                print(traceback.format_exc())
                executor.shutdown(wait=False, cancel_futures=True)
                kill_child_processes(os.getpid())
            else:
                print('%r completed' % (proc, ))
                executor.shutdown(wait=False, cancel_futures=True)
                kill_child_processes(os.getpid())
    """ """


# Init logging system
def get_log_file_name():
    current_date = datetime.now()
    # log_dir = "./logs/" + current_date.strftime("%Y-%m/")
    log_dir = "./logs/"
    os.makedirs(log_dir, exist_ok=True)
    # log_file = "{:0>2}".format(current_date.strftime("%d_%H%M%S") + "_main.log")
    log_file = "main_{:0>2}".format(current_date.strftime("%Y-%m-%d") + ".log")
    return log_dir + log_file


def log_init():
    logger = logging.getLogger()
    base_log_file_name = get_log_file_name()

    log_handler = TimedRotatingFileHandler(
        base_log_file_name,
        when="midnight",
        interval=1,
        backupCount=0,
        encoding="utf8",
        delay=False,
        utc=False,
        atTime=None
    )

    log_handler.namer = "_get_log_file_name"

    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    log_handler.setFormatter(log_formatter)
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(log_handler)

    logger.info("-=== Application started ===---")

    logging.getLogger('pika').setLevel(logging.ERROR)



def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return

    children = parent.children(recursive=True)

    for process in children:
        process.send_signal(sig)


if __name__ == "__main__":
    main()
