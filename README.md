# Initial Setup
```
export AIRFLOW_HOME=$(pwd)
# This have been done by .env + pipenv shell

# init airflow database
airflow initdb

# start the web server, default port is 8080
airflow webserver -p 8080

# start the scheduler
airflow scheduler
```


