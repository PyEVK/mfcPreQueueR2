import datetime
import pytz

# TimeZone of project
EKT = pytz.timezone('Asia/Yekaterinburg')


def is_duplicate(item1, item2):
    key_fields = [
        "mfc_number",
        "mfc_date",
        "mfc_time_from",
        "mfc_phone"
    ]

    for key in key_fields:
        try:
            if item1[key] != item2[key]:
                return False
        except KeyError:
            continue

    return True


def next_call_time(mfc_date, mfc_time_from, retry_count, last_call_date, scheduler):
    if retry_count > 0:
        mplayer = 2 ** (retry_count - 1)
        waiting_minutes = mplayer * scheduler["retry_waiting_minutes"]
        result = (last_call_date + datetime.timedelta(minutes=waiting_minutes)).astimezone(EKT)
    else:
        mfc_time = f"{mfc_date} {mfc_time_from}"
        result = datetime.datetime.strptime(mfc_time, "%Y-%m-%d %H:%M").astimezone(EKT)

    dt_now = datetime.datetime.now(EKT)
    if result < dt_now:
        return None
    else:
        return result
