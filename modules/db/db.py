# Database Helper
import psycopg2.extras
import yaml

DB_CONF_FILE_PATH = './conf/db.yaml'


def connect():
    with open(DB_CONF_FILE_PATH, 'r') as db_conf_file:
        db_conf = yaml.safe_load(db_conf_file)

    try:
        return psycopg2.connect(**db_conf, connect_timeout=10)
    except psycopg2.Error:
        return None


def register(dbh, data):
    sql_params = (
        data['uuid'],
        data['number'],
        data['dateVisit'],
        data['dateVisitTimeBeg'],
        data['dateVisitTimeEnd'],
        data['phone'],
        data['status'],
        data['pbx_phone']
    )

    sql_query = """
        INSERT INTO public.prequeue_stat (
            uuid,
            mfc_number,
            mfc_date,
            mfc_time_from,
            mfc_time_before,
            mfc_phone,
            mfc_state,
            phone,
            created_at) VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, NOW());
    """

    with dbh.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql_query, sql_params)
