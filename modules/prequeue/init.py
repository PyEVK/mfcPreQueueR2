import psycopg2


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
    cnt = 0
    with dbh.cursor() as cur:
        cur.execute(sql_query, sql_params)
        for row in cur:
            print(row)
            cnt += 1

    print(cnt)
