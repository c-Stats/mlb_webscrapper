from setuptools import find_packages, setup
from package import Package

import pandas as pd
import requests
import urllib
import numpy as np

import os.path
from os import path

from bs4 import BeautifulSoup

from datetime import datetime
from datetime import timedelta

import time
import random

from tqdm import tqdm

import sys 

from os import listdir
from os.path import isfile, join

import re

pd.options.mode.chained_assignment = None

setup(
    name = 'mlb_webscrapper',
    url = 'https://github.com/c-Stats/mlb_webscrapper',
    author = 'Francis F.',
    author_email = 'frankfredj@gmail.com',

    packages = find_packages(),

    install_requires = ["pandas", "requests", "bs4",
                "numpy", "tqdm", "path"],

    version = '0.1',
 
    license = 'none',
    description = 'webscrapes MLB player stats and scores'

)


