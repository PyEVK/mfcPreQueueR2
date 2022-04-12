import logging

import psycopg2
from modules.helpers import prequeue


def init(dbh, lst):
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
            call_result,
            created_at,
            updated_at,
            deleted_at
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
                call_result, \
                created_at, \
                updated_at, \
                deleted_at = row

            next_call_time = \
                prequeue.next_call_time(mfc_date, mfc_time_from, retry_count, last_call_date, scheduler=None)

            if next_call_time is None:
                logging.info("Too old record %s (%s %s)", uuid, mfc_date, mfc_time_from)
#                upd_sql = """
#                    UPDATE public.prequeue_stat
#                    SET call_result = 2000, updated_at = NOW()
#                    WHERE id = %s
#                    """
#                with dbh.cursor() as upd_cur:
#                    upd_cur.execute(upd_sql, (row_id, ))

                continue

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
                    "call_result": call_result,
                    "next_call_time": next_call_time
                }

            if mfc_number in lst:
                for idx in lst[mfc_number]:
                    if prequeue.is_duplicate(idx, row_item):
                        logging.info("Duplicate row:::%s,%s,%s,%s",
                                     idx["mfc_number"], idx["mfc_date"], idx["mfc_time_from"], idx["mfc_phone"])
                        print("Duplicate row", row_item)
                    else:
                        lst_item = lst[mfc_number]
                        lst_item.append(row_item)
                        lst[mfc_number] = lst_item
            else:
                lst[mfc_number] = [row_item]

    from pprint import pprint
    pprint(dict(lst))


