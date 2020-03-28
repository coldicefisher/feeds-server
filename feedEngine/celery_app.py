from celery import Celery
from celery.schedules import crontab


app = Celery('feedEngine',
    broker='pyamqp://rabbitmq:5672',
    backend="db+postgresql+psycopg2://postgres:postgres@postgres_celery:5432",
    # include=['feed_tasks']
    )

# app.conf.beat_schedule = {
#     "get-historical-data": {
#         "task": "feed_tasks.get_historical_data",
#         "schedule": crontab(minute=4, hour=14)
#     }
# }
    #print (app.control.inspect().stats().keys())
    #print (app.control.inspect().ping())
    #app.worker_main(argv)

