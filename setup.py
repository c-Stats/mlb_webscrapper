from setuptools import setup

setup(
    name = 'mlb_webscrapper',
    url = 'https://github.com/c-Stats/mlb_webscrapper',
    author = 'Francis F.',
    author_email = 'frankfredj@gmail.com',

    packages = ['mlb_webscrapper'],

    install_requires = ["pandas", "requests", "urllib",
                "numpy", "os", "datetime",
                "time", "random", "tqdm", "sys",
                "re"],

    version = '0.1',
 
    license = 'none',
    description = 'webscrapes MLB player stats and scores',

)