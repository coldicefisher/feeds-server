from feedEngine import feed_tasks
import os

# This is ran everytime the server reloads. Using an environment variable to control the startup doesn't work.
def run():
    if feed_tasks.num_of_workers() == 0: feed_tasks.start_worker() # start a worker and flower
    