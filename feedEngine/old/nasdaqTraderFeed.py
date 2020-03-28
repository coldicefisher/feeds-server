import sys
sys.path.append("..") # Adds higher directory to python modules path.

import json, requests, logging, os, re, time, datetime, ftplib
from helpers import *
import settings


import bs4 as bs



feed = NasdaqTraderParser()
feed.get_stock_symbols()