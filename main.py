from datetime import datetime
import psycopg2
import json


def execute(command: str) -> bool:
    db_info = json.load(open('db.json'))
    with psycopg2.connect(database=db_info['database'], user=db_info['user'],
            password=db_info['password'], host=db_info['host'], port=db_info['port']) as conn:
        cursor = conn.cursor()
        cursor.execute(command)
        conn.commit()
        rowcount = cursor.rowcount > 0
        conn.close()
        return rowcount

def log_error(error_sql: str):
    with open('logfile', 'a') as logfile:
        logfile.write(f'\nError at {datetime.now()}:  {error_sql}')


if __name__ == '__main__':
    # read the file, see if any commands need to execute
    command_file = open('commands.txt').read()
    for line in command_file.split('\n')[1:]:
        last_run_time, interval_s, sql = line.split('\t')
        elapsed = datetime.now() - datetime.fromisoformat(last_run_time)
        if elapsed.total_seconds() > float(interval_s):
            executed = execute(sql)
            if executed:
                command_file = command_file.replace(str(last_run_time),
                     f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f')}")
            else:
                log_error(sql)
    with open('commands.txt', 'w') as new_file:
        new_file.write(command_file)
