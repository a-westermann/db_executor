# db_executor
quick and dirty psql update script that reads from a 'commands.txt' file containing these columns (incl. header):


last_run_time  interval_s  sql


datetime  300  update table set foo = 'bar';
