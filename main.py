from datetime import datetime
import psycopg2
import json


def execute(command: str):
    print(f'executing: {command}')
    db_info = json.load(open('db.json'))
    conn = psycopg2.connect(database=db_info['database'], user=db_info['user'],
            password=db_info['password'], host=db_info['host'], port=db_info['port'])
    cursor = conn.cursor()
    cursor.execute(command)
    conn.commit()
    print(f'Found {cursor.rowcount} records to update/delete')
    conn.close()


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
    with open('commands.txt', 'w') as new_file:
        new_file.write(command_file)
