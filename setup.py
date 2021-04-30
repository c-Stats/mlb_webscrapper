from setuptools import find_packages, setup
from package import Package

setup(
    name = 'mlb_webscrapper',
    url = 'https://github.com/c-Stats/mlb_webscrapper',
    author = 'Francis F.',
    author_email = 'frankfredj@gmail.com',

    packages = find_packages(),
    py_module = ["mlb_webscrapper", "path"],

    install_requires = ["pandas", "requests", "bs4",
                "numpy", "tqdm", "path"],

    cmdclass={
        "package": Package
    }

    version = '0.1',
 
    license = 'none',
    description = 'webscrapes MLB player stats and scores',

)


