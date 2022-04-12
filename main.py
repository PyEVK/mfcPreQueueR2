import logging
from logging.handlers import TimedRotatingFileHandler
import os
import pathlib
from datetime import datetime
import pytz
from modules import db
from modules import amqp
from modules import prequeue
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Set working directory to file location
os.chdir(pathlib.Path(__file__).parent.absolute())

# Set logging level for project
LOG_LEVEL = logging.INFO

# TimeZone of project
EKT = pytz.timezone('Asia/Yekaterinburg')


def main():
    log_init()

    # Connect to Database
    print("Database connection")
    dbh = db.connect()
    if dbh is None:
        print("Database connection error")
        logging.error("Database connection error")
        return None

    # Connect to AMQP and get channel
    print("AMQP connection")
    amqp_channel = amqp.connect()

    # Prepare for multiprocessing
    print("Initialization")
    manager = multiprocessing.Manager()
    prequeue_lst = manager.dict()
    call_counter = manager.Value('i', 0)
    evt_queue_new_item = manager.Event()
    evt_upd_scheduler = manager.Event()

    # Init list
    prequeue.init(dbh, prequeue_lst)
    dbh.close()

    print("Prepare for launch subprocesses")
    executor = ProcessPoolExecutor()


# Init logging system
def get_log_file_name():
    current_date = datetime.now()
    log_dir = "./logs/" + current_date.strftime("%Y-%m/")
    os.makedirs(log_dir, exist_ok=True)
    # log_file = "{:0>2}".format(current_date.strftime("%d_%H%M%S") + "_main.log")
    log_file = "{:0>2}".format(current_date.strftime("%d") + "_main.log")
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


if __name__ == "__main__":
    main()
