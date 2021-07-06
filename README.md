# mlb_webscrapper

To install the package, simply do:

``` python

pip install git+https://github.com/c-Stats/mlb_webscrapper.git#egg=mlb_webscrapper

``` 

Once this is done, you can call the webscrapper, i.e.:

``` python

from mlb_webscrapper import webscrapper

#Your file path here
directory = "D:/MLB"
scrapper = webscrapper.Baseball_Scrapper(directory)

``` 

To use the webscrapper:

# Initialising a new database

```python
#Workflow to initialise a database of player statistics, and scores.

#Extract a list of match urls over a given date range
scrapper.Get_FanGraphs_Game_URLs(frm, to)

#Webscrappe the aforementioned urls.
scrapper.Extract_FanGraphs_Box_Scores()
```

# Data pre-processing (cleaning)

```python
#Builds the final database to be fed to R programs.
#Cleans data and adds the predicted lineups

scrapper.Clean_Data()
scapper.Scrape_BASEBALL_REFERENCE_lineups()
```

# Updating the current database

```python
#Webscrapes newly avaible data
#Pre-process the data if new data was found

scrapper.UPDATE_FanGraphs_Box_Scores()
```

# Web-scraping LotoQuebec's and Pinnacle's moneylines

```python
#Webscrapes the current moneylines offered on Loto-Quebec's Mise-o-jeu gambling website.

scrapper.Betting_Webscrape()
```

# NOTE:

In order to make use of the following package, one must copy the Abreviations_Dictionary.csv file at path\MLB_Modeling\Misc.
