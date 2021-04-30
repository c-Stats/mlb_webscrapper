from setuptools import find_packages, setup


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


