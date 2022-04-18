import logging
from modules.helpers import prequeue


def get_init_lst(dbh, lst):
    sql_params = ()
    sql_query = """
        SELECT
            id,
            uuid,
            mfc_number,
            mfc_date,
            mfc_time_from,
            mfc_time_before,
            mfc_phone,
            mfc_state,
            phone,
            last_call_date,
            last_call_disposition,
            retry_count,
            call_result
        FROM public.prequeue_stat
        WHERE 
            coalesce(call_result, 0) = 0
            and deleted_at IS NULL 
    """

    lst.clear()
    with dbh.cursor() as cur:
        cur.execute(sql_query, sql_params)
        for row in cur:
            row_id, \
                uuid, \
                mfc_number, \
                mfc_date, \
                mfc_time_from, \
                mfc_time_before, \
                mfc_phone, \
                mfc_state, \
                phone, \
                last_call_date, \
                last_call_disposition, \
                retry_count, \
                call_result = row

            row_item = {
                    "id": row_id,
                    "uuid": uuid,
                    "mfc_number": mfc_number,
                    "mfc_date": mfc_date,
                    "mfc_time_from": mfc_time_from,
                    "mfc_time_before": mfc_time_before,
                    "mfc_phone": mfc_phone,
                    "phone": phone,
                    "last_call_date": last_call_date,
                    "last_call_disposition": last_call_disposition,
                    "retry_count": retry_count,
                    "call_result": call_result
                }

            lst[uuid] = row_item


def get_settings(dbh, settings):
    sql_params = ()
    sql_query = """
            SELECT
                id,
                settings
            FROM public.prequeue_settings
            WHERE 
                id = 1 
        """

    with dbh.cursor() as cur:
        cur.execute(sql_query, sql_params)
        row_id, row_settings = cur.fetchone()

    row_settings['download_time'] = "08:12"
    row_settings['download_days'] = "5"

    settings["settings"] = row_settings
