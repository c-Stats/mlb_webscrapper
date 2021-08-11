import pandas as pd
import requests
import urllib
import numpy as np

import os.path

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

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

from dateutil import parser
import functools

pd.options.mode.chained_assignment = None


class Baseball_Scrapper:

    def __init__(self, file_path):

        self.file_path = file_path
        dir_path = file_path + "/MLB_Modeling"

        #Create the repositories if they do not exist
        #Main repo
        target = file_path + "/MLB_Modeling"
        if not os.path.exists(target):
            os.mkdir(target)
            print("Main directory created at:" + "\t" + target)

        #Sub-repo
        sub_directories = ["Bat", "Pitch", "Scores", "Betting", "Misc"]
        for element in sub_directories:

            target = dir_path + "/" + element
            if not os.path.exists(target):
                os.mkdir(target)
                print("Sub-directory created at:" + "\t" + target)


        #Sub-repo locations
        self.paths = []
        for element in sub_directories:
            self.paths.append(dir_path + "/" + element)


        dictio_path = self.paths[4] + "/Abreviations_Dictionary.csv"
        if os.path.exists(dictio_path):
            self.dictio = pd.read_csv(dictio_path)
        else:
            self.dictio = []
            print("MISSING FILE AT:" + str(dictio_path))


        print("Scapper succesfully initiated.")


    #Updates a file, or save it if it doesn't exists
    def update_file(self, save_path, file_name, data):

        final_path = save_path + "/" + file_name

        try:
            if not os.path.exists(final_path):
                if len(data) > 0:
                    data.to_csv(final_path, index = False)

            else:
                if len(data) > 0:
                    pd.read_csv(final_path).append(data).drop_duplicates().to_csv(final_path, index = False)

        except:
            print("Failed to update file.") 



    #Translates a team name to its city name
    def Translate_Team_Names(self, value, want):

        value = str(value)

        if len(self.dictio) == 0:
            sys.exit("Missing file:" + "\t" + self.paths[4] + "Abreviations_Dictionary.csv")
        else:
            x = self.dictio

        m = len(x.columns)


        for j in range(0, m):
            location = np.where(x.iloc[:, j] == value)[0]

            if len(location) > 0:
                return x.at[location[0], want]


    #Frame-wide translate
    def Fix_Team_Names(self, frame, want):

        for col in frame.columns:

            if "Team" in col or "Opponent" in col:

                vector = np.array(list(frame[col]))
                values = np.array(list(set(vector)))

                m = np.where(frame.columns == col)[0][0]

                for team in values:

                    index = np.where(vector == team)[0]
                    proper_name = self.Translate_Team_Names(team, want)
                    
                    frame.iloc[index, m] = proper_name

        return frame


    #Function to produce unique match IDs
    def numerise_string(self, x):

        alphabet = "abcdefghijklmnopqrstuvwxyz"
        tag = ""

        for letters in x:
            tag = tag + str(alphabet.find(letters.lower()))

        return tag


    #Name translator
    def find_name(self, name, team, all_names):

        if str(name) == "nan":
            return "None"

        current = "None"

        dummy = np.char.lower(all_names)

        family_name = name.split(" ")[-1]
        first_letter = name[0]

        while True:

            #First Search
            search_val = (name.replace(" ", "") + team).lower()
            matches = np.where(dummy == search_val)[0]
            if len(matches) > 0:
                break

            #Second search, by family name
            search_val = (family_name + team).lower()
            matches = []
            for i in range(0, len(dummy)):
                if search_val in dummy[i]:
                    matches.append(i)

            if len(matches) > 0:
                break

            #Third search, with no alphanumerics
            search_val = "".join([x for x in search_val if x.isalpha()])
            matches = []
            for i in range(0, len(dummy)):
                if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
                    matches.append(i)   

            if len(matches) > 0:
                break   


            #Added: family name, no team
            search_val = (family_name).lower()  
            matches = []
            for i in range(0, len(dummy)):
                if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
                    matches.append(i)   

            if len(matches) > 0:
                break                       


            #Added: search for junior
            search_val = (family_name + "jr").lower()   
            matches = []
            for i in range(0, len(dummy)):
                if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
                    matches.append(i)   

            if len(matches) > 0:
                break   



            #Alternative search, with possible 2nd family name
            family_name = name.split(" ")[-2]

            #3rd search, by family name
            search_val = (family_name + team).lower()
            matches = []
            for i in range(0, len(dummy)):
                if search_val in dummy[i]:
                    matches.append(i)

            if len(matches) > 0:
                break   

            #4th, with no alphanumerics
            search_val = "".join([x for x in search_val if x.isalpha()])
            matches = []
            for i in range(0, len(dummy)):
                if search_val in "".join([x for x in dummy[i] if x.isalpha()]):
                    matches.append(i)           

            if len(matches) > 0:
                break   

            break


        if len(matches) > 0:
            for i in matches:
                if all_names[i][0] == first_letter and all_names[i][-3:] == team:
                    current = all_names[i]

        return current


    ###########################################################
    ###########################################################
    ######################## WORK FLOW ########################
    ###########################################################
    ###########################################################


    ###########################################################
    #################### WEB SCRAPPING ########################
    ###########################################################


    #Attempts to extract game URLs from a certain date
    #Is used inside a loop
    def Scrape_FanGraphs_game_url(self, date):

        url = "https://www.fangraphs.com/scoreboard.aspx?date=" + date

        html = requests.get(url).content
        html_content = BeautifulSoup(html, 'lxml')

        links = html_content.findAll('a')
        game_url = []

        for link in links:
            try:
                href = link.attrs['href']
            except:
                continue

            if "boxscore" in href:
                game_url.append("https://www.fangraphs.com/" + href)

        return game_url


    #Scrapes FanGraphs.com for urls to games that were played between to dates (frm, to)
    #Is used to initiate the database
    #Once done, the UPDATE_FanGraphs_Box_Scores method should be used
    def Get_FanGraphs_Game_URLs(self, frm, to):

        begin = datetime.strptime(frm, "%Y-%m-%d")
        end = datetime.strptime(to, "%Y-%m-%d")

        n = (end - begin).days + 1

        urls = pd.DataFrame(columns = ["URL"])
        no_games_dates = pd.DataFrame(columns = ["Dates"])
        games_dates = pd.DataFrame(columns = ["Dates"])


        #Check for dates which links were already scrapped
        if os.path.exists(self.paths[-1] + "/Game_Dates.csv"):
            dates_done = list(pd.read_csv(self.paths[-1] + "/Game_Dates.csv")["Dates"])
        else:
            dates_done = []

        #Main loop (extraction + auto-save)
        for i in tqdm(range(0, n)):

            date = datetime.strftime(begin, "%Y-%m-%d")

            #Avoid extracting for certain cases
            if (begin.month < 3) or (begin.month > 10) or (date in dates_done):
                begin = begin + timedelta(days = 1)
                continue

            #Retrieve links
            try:
                todays_url = self.Scrape_FanGraphs_game_url(date)
            except:
                no_games_dates = no_games_dates.append(pd.DataFrame(date, columns = ["Dates"]))
                begin = begin + timedelta(days = 1)
                continue

            if len(todays_url) > 0:
                urls = urls.append(pd.DataFrame(todays_url, columns = ["URL"]))
                games_dates = games_dates.append(pd.DataFrame([date], columns = ["Dates"])) 

                print("Scrapped:" + "\t" + date)


            #Saving procedure (trigerred every 20 iterations)
            if (i + 1) % 20 == 0 or begin == end:

                self.update_file(self.paths[-1], "Game_URLs.csv", urls)
                urls = pd.DataFrame(columns = ["URL"])

                self.update_file(self.paths[-1], "No_Game_Dates.csv", no_games_dates)
                no_games_dates = pd.DataFrame(columns = ["Dates"])

                self.update_file(self.paths[-1], "Game_Dates.csv", games_dates)
                games_dates = pd.DataFrame(columns = ["Dates"])         

                print("Saved data.")


            begin = begin + timedelta(days = 1)
            time.sleep(random.randint(3, 5))

        print("Done.")



    #Get the Box Scores data based off a URL 
    def Scrape_FanGraphs_game_stats_by_url(self, ulr):

        html = requests.get(url).content
        tables = pd.read_html(html)

        #Date and team names
        url_split = url.split("-")
        date = url_split[0].split("=")[-1] + "-" + url_split[1] + "-" + url_split[2].split("&")[0]

        date_index = -1
        for table in tables:
            date_index += 1
            if table.iloc[0,0] == "Team":
                break   

        home_team = tables[date_index].iloc[2, 0]
        away_team = tables[date_index].iloc[1, 0]   

        #Score
        home_score = tables[date_index].iloc[2, -1]
        away_score = tables[date_index].iloc[1, -1]

        ID = ""
        temp = date.split("-")
        for values in temp:
            ID = ID + values

        ID = ID + self.numerise_string(home_team[0:2] + away_team[0:2])

        scores = pd.DataFrame(columns = ["Home", "Home_Score", "Away", "Away_Score", "Date", "URL", "ID"])
        scores.loc[0] = [home_team, home_score, away_team, away_score, date, url, ID]


        #Find where the extraction should begin
        start = 0
        for table in tables:
            start += 1
            if str(type(table.columns)) == "<class 'pandas.core.indexes.multi.MultiIndex'>":
                break

        tables = tables[start:]

        #Find the play by play table
        table_lengths = []
        for table in tables:
            table_lengths.append(len(table))

        table_lengths = np.array(table_lengths)

        play_by_play_index = np.where(table_lengths == np.max(table_lengths))[0][0]
        play_by_play = tables[play_by_play_index]
        del tables[play_by_play_index]
        table_lengths = np.delete(table_lengths, play_by_play_index)

        #Merge the frames
        merged_tables = []
        for i in range(0, 4):

            temp_table = tables[i]
            for j in range(4, len(tables)):

                size = len(temp_table)

                if len(tables[j]) == size:

                    check = len(np.where(tables[i]["Name"] == tables[j]["Name"])[0])
                    if check == size:

                        temp_table = pd.merge(temp_table, tables[j], on = "Name")

            temp_table["Date"] = date
            if i % 2 == 0:
                temp_table["Team"] = home_team
                temp_table["Location"] = "Home"
                temp_table["Opponent"] = away_team
            else:
                temp_table["Team"] = away_team
                temp_table["Location"] = "Away"
                temp_table["Opponent"] = home_team

            colnames = []
            for j in range(0, len(temp_table.columns)):
                colnames.append(temp_table.columns[j].split("_")[0])

            temp_table.columns = colnames
            temp_table["ID"] = ID

            merged_tables.append(temp_table.loc[:,~temp_table.columns.duplicated()])


        merged_tables.append(scores)

        return merged_tables



    #Extracts the box scores based off the URL list
    def Extract_FanGraphs_Box_Scores(self):

        url_path = self.paths[-1] + "/Game_URLs.csv"
        if os.path.exists(url_path):

            urls = list(set(list(pd.read_csv(url_path)["URL"])))
            ID = 0

            #Checks for existing Box_Scores
            path_to_check = self.paths[2] + "/FanGraphs_Scores.csv"
            if os.path.exists(path_to_check):

                temp = pd.read_csv(path_to_check)

                urls_done = list(temp["URL"])
                urls = [x for x in urls if x not in urls_done]

                ID = np.max(temp["ID"]) + 1

            #Sort by date
            dates = [x.split("date=")[-1].split("&")[0] for x in urls]
            urls = np.array(urls)[np.argsort(dates)]

            #Initialise variables

            bat = []
            pitch = []
            scores = []

            count = 0
            n = len(urls)

            print("Extracting " + str(n) + " Box Scores...")
            #e_time = round((((45/2) + 3) * n) / 60, 2)
            #print("Estimated running time:" + "\t" + str(e_time) + " minutes")

            #Loop throught URLs 
            for i in tqdm(range(0, n)):

                url = str(urls[i])
                count += 1
                try:

                    html = requests.get(url).content
                    tables = pd.read_html(html)

                    #Date and team names
                    url_split = url.split("-")
                    date = url_split[0].split("=")[-1] + "-" + url_split[1] + "-" + url_split[2].split("&")[0]

                    date_index = -1
                    for table in tables:
                        date_index += 1
                        if table.iloc[0,0] == "Team":
                            break   

                    home_team = tables[date_index].iloc[2, 0]
                    away_team = tables[date_index].iloc[1, 0]   

                    #Score
                    home_score = tables[date_index].iloc[2, -1]
                    away_score = tables[date_index].iloc[1, -1]

                    ID2 = ""
                    temp = date.split("-")
                    for values in temp:
                        ID2 = ID2 + values

                    ID2 = ID2 + self.numerise_string(home_team[0:2] + away_team[0:2])

                    scores2 = pd.DataFrame(columns = ["Home", "Home_Score", "Away", "Away_Score", "Date", "URL", "ID"])
                    scores2.loc[0] = [home_team, home_score, away_team, away_score, date, url, ID2]


                    #Find where the extraction should begin
                    start = 0
                    for table in tables:
                        start += 1
                        if str(type(table.columns)) == "<class 'pandas.core.indexes.multi.MultiIndex'>":
                            break

                    tables = tables[start:]

                    #Find the play by play table
                    table_lengths = []
                    for table in tables:
                        table_lengths.append(len(table))

                    table_lengths = np.array(table_lengths)

                    play_by_play_index = np.where(table_lengths == np.max(table_lengths))[0][0]
                    play_by_play = tables[play_by_play_index]
                    del tables[play_by_play_index]
                    table_lengths = np.delete(table_lengths, play_by_play_index)

                    #Merge the frames
                    merged_tables = []
                    for i in range(0, 4):

                        temp_table = tables[i]
                        for j in range(4, len(tables)):

                            size = len(temp_table)

                            if len(tables[j]) == size:

                                check = len(np.where(tables[i]["Name"] == tables[j]["Name"])[0])
                                if check == size:

                                    temp_table = pd.merge(temp_table, tables[j], on = "Name")

                        temp_table.loc[:, "Date"] = date
                        if i % 2 == 0:
                            temp_table.loc[:, "Team"] = home_team
                            temp_table.loc[:, "Location"] = "Home"
                            temp_table.loc[:, "Opponent"] = away_team
                        else:
                            temp_table.loc[:, "Team"] = away_team
                            temp_table.loc[:, "Location"] = "Away"
                            temp_table.loc[:, "Opponent"] = home_team

                        colnames = []
                        for j in range(0, len(temp_table.columns)):
                            colnames.append(temp_table.columns[j].split("_")[0])

                        temp_table.columns = colnames
                        temp_table.loc[:, "ID"] = ID

                        merged_tables.append(temp_table.loc[:,~temp_table.columns.duplicated()])


                    merged_tables.append(scores2)

                    tables = merged_tables
                    for k in range(0, len(tables)):
                        tables[k].loc[:, "ID"] = ID

                except:
                    print("ERROR")
                    time.sleep(random.randint(2,4))
                    ID += 1
                    continue

                if len(bat) == 0:
                        bat = tables[0].append(tables[1], sort = True)
                else:
                    bat = bat.append(tables[0].append(tables[1], sort = True), sort = True)

                if len(pitch) == 0:
                        pitch = tables[2].append(tables[3], sort = True)
                else:
                    pitch = pitch.append(tables[2].append(tables[3], sort = True), sort = True)

                if len(scores) == 0:
                        scores = tables[4]
                else:
                    scores = scores.append(tables[4], sort = True)


                print("\t" + "\t" + "\t" + "***** ADDED GAME *****")
                print(scores.iloc[-1,:])

                ID += 1

                #print(scores)

                if (count + 1) % 100 == 0 or url == urls[-1]:

                    self.update_file(self.paths[0], "FanGraphs_Box_Scores.csv", bat)    
                    bat = []

                    self.update_file(self.paths[1], "FanGraphs_Box_Scores.csv", pitch)  
                    pitch = []                  

                    self.update_file(self.paths[2], "FanGraphs_Scores.csv", scores) 
                    scores = []

                    print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

                if url != urls[-1]:
                    time.sleep(random.randint(2, 4))


    #Extracts the box scores based off the URL list
    def Extract_FanGraphs_Play_by_play(self):

        #Extract the scores
        scores_path = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
        if not os.path.exists(scores_path):

            print("Error: no game scores have been scraped and/or cleaned yet.")
            print("Missing file at: " + scores_path)

            return 0

        scores = pd.read_csv(scores_path)
        scores["Year"] = pd.DatetimeIndex(scores["Date"]).year

        #Check if the directory where the file are to be saved exists
        saving_dir = self.paths[2] + "/Play_by_play"
        if not os.path.exists(saving_dir):

            print("Creating directory at:" + saving_dir)
            os.mkdir(saving_dir)


        #Check matches that were already processed
        processed_ID = []
        files_in_dir = [f for f in listdir(saving_dir) if isfile(join(saving_dir, f))]

        if len(files_in_dir) > 0:

            flush = []

            for file in files_in_dir:

                #...
                temp = pd.read_csv(saving_dir + "/" + file)
                ids_done = list(set(list(temp["ID"])))

                to_remove = [i for i, x in enumerate(list(scores["ID"])) if x in ids_done]
                scores = scores.drop(to_remove).reset_index(drop = True)
                

        #Loop over seasons
        seasons = np.sort(list(set(list(scores["Year"]))))
        for season in seasons:

            print("Processing data from year " + str(season) + "...")

            #Loop over matches
            to_process = scores.loc[np.where(scores["Year"] == season)[0]].reset_index(drop = True)

            n_matches = len(to_process)
            e_wait_time_min = round(5 * n_matches / 60)

            h = int(np.floor(e_wait_time_min / 60))
            e_wait_time_min -= 60 * h

            print("Estimated processing time: " + str(h) + " hour(s) and " + str(int(e_wait_time_min)) + " minutes(s).")



            all_plays = []
            for i in tqdm(range(0, len(to_process))):

                url = to_process.at[i, "URL"]

                try:

                    html = requests.get(url).content
                    tables = pd.read_html(html)

                    tbl_len = [len(x) for x in tables]
                    at = tbl_len.index(max(tbl_len))

                    play_by_play = tables[at]

                    for col in to_process.columns:
                        play_by_play[col] = to_process.at[i, col]

                except:
                    print("ERROR")
                    time.sleep(random.randint(2,4))

                    continue

                if len(all_plays) == 0:
                        all_plays = play_by_play
                else:
                    all_plays = all_plays.append(play_by_play, sort = True, ignore_index = True)

                if (i + 1) % 100 == 0 or i == len(to_process) - 1:          

                    self.update_file(saving_dir, str(season) + ".csv", all_plays)   
                    all_plays = []

                    print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

                time.sleep(random.randint(2,4))

    ###########################################################
    #################### DATA CLEANING  #######################
    ###########################################################

    #Cleans the bat, pitch and scores frames
    def Clean_Data(self):

        #Create sub-repositories if they do not already exist
        sufix = "/Clean_Data"

        for i in range(0, (len(self.paths) - 1)):
            path_string = self.paths[i] + sufix
            if not os.path.exists(path_string):
                os.mkdir(path_string)
                print("Create sub-directory at:" + "\t" + path_string)

        scores_path = self.paths[2] + "/FanGraphs_Scores.csv"
        if not os.path.exists(scores_path):
            sys.exit("No data to clean.")
        else:
            scores = pd.read_csv(scores_path)[["Home", "Home_Score", "Away", "Away_Score", "Date", "URL", "ID"]]

        scores.columns = ["Team_Home", "Score_Home", "Team_Away", "Score_Away", "Date", "URL", "ID"]

        #Load bat and pitch frames
        frames = []
        for i in range(0,2):

            path_string = self.paths[i] + "/FanGraphs_Box_Scores.csv"
            if not os.path.exists(path_string):
                sys.exit("Missing file:" + "\t" + path_string)
            else:
                frames.append(pd.read_csv(path_string, dtype={'a': str})) 


        #Use CITY abreviations for TEAMS
        scores = self.Fix_Team_Names(scores, "City")
        for i in range(0,2):
            frames[i] = self.Fix_Team_Names(frames[i], "City")


        #Tag starting pitchers
        print("Tagging Starting Pitchers ...")
        frames[1]["Starting"] = "No"
        IDs = list(scores["ID"])
        for i in tqdm(range(0, len(IDs))):

            ID = IDs[i]
            index_match = np.where(frames[1]["ID"] == ID)[0]
            if len(index_match) == 0:
                continue

            teams = list(set(list(frames[1]["Team"][index_match])))
            for team in teams:
                starting = index_match[np.where(frames[1]["Team"][index_match] == team)[0][0]]
                frames[1].at[starting, "Starting"] = "Yes"



        for i in range(0, 2):

            x = frames[i]

            #Remove "Total" rows
            rmv = np.where(x["Name"] == "Total")[0]
            x = x.drop(rmv).reset_index(drop = True)


            #Replace NaNs with 0
            n_NaNs = x.isna().sum()
            fix = np.where(n_NaNs > 0)[0]
            cols_to_fix = x.columns[fix]

            if len(fix) > 0:
                for cnames in cols_to_fix:

                    #Replace with 0
                    col_index = np.where(x.columns == cnames)[0][0]
                    to_replace = np.where(x[cnames].isna())[0]

                    if "%" in cnames or cnames == "HR/FB":
                        x.iloc[to_replace, col_index] = "0.0%"
                    else:
                        if x[cnames].dtype == np.float64:
                            x.iloc[to_replace, col_index] = 0.0
                        else:
                            x.iloc[to_replace, col_index] = 0



            #Format percentages
            data_types = list(x.dtypes)
            for j in range(0, len(x.columns)):
                if data_types[j] == np.float64 or data_types[j] == np.int64:
                    continue
                
                else:
                    m = x.columns[j]

                    if ("%" in m and not "NaN" in m) or m == "HR/FB":
                        try:
                            x[m] = x[m].str.replace("%", "").astype(float) / 100
                        except:
                            problem = [k for k, x in enumerate(list(x[m])) if "," in x] 
                            index_col = np.where(x.columns == m)[0][0]
                            x.iloc[problem, index_col] = "0.0%"

                            x[m] = x[m].str.replace("%", "").astype(float) / 100

                    else:
                        try:
                            x[m] = x[m].astype(float)
                        except:
                            continue
    


            #Add position variable
            #Only for bat
            if i == 0:

                splitted_names = pd.DataFrame(list(x["Name"].str.split(" - ")), columns = ["Name", "Position"])

                x["Name"] = (splitted_names["Name"] + " " +  x["Team"]).str.replace(" ", "")

                temp = list(set(list(splitted_names["Position"])))

                positions = list()
                for values in temp:

                    try:
                        y = values.split("-")
                    except:
                        continue

                    for vals in y:
                        if not vals in positions:
                            positions.append(vals)

                position_names = []

                for values in positions:
                    c_name = "Position_" + values
                    x[c_name] = 0
                    position_names.append(c_name)


                for j in range(0, len(x)):

                    try:
                        y = splitted_names["Position"][j].split("-")
                    except:
                        continue
                        
                    for values in y:
                        c_name = "Position_" + values
                        x.at[j, c_name] = 1

                frames[i] = x.sort_values("Date", ascending=False)

            else:

                splitted_names = pd.DataFrame(list(x["Name"].str.split("(")), columns = ["Name", "Position"])
                x["Name"] = (splitted_names["Name"] + " " +  x["Team"]).str.replace(" ", "")
                frames[i] = x.sort_values("Date", ascending=False)


        scores = scores.sort_values("Date", ascending = False)
        for i in range(0, 2):
            frames[i] = frames[i].sort_values("Date", ascending = False)

        #Save the cleaned files
        for i in range(0, 2):
            save_path = self.paths[i] + "/Clean_Data/FanGraphs_Box_Scores.csv"
            frames[i].to_csv(save_path, index = False)
            print("Saved:" + "\t" + save_path)

        save_path = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
        scores = scores.sort_values("Date", ascending=False)
        scores.to_csv(save_path, index = False)
        print("Saved:" + "\t" + save_path)


        print("Cleaning done.")


    #Cleans betting data
    def Clean_Betting_Data(self):

        #Set sub-directory up
        path_data = self.paths[3] + "/Clean_Data"
        if not os.path.exists(path_data):
            os.mkdir(path_data)
            print("Created sub-directory at:" + "\t" + path_data)

        #Extract CSV files if needed
        url = "https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/mlboddsarchives.htm"
        html = requests.get(url).content
        html_content = BeautifulSoup(html, 'lxml')
        links = html_content.findAll('a')

        file_url = []
        for link in links:
            try:
                href = link.attrs['href']
            except:
                continue

            if ".xlsx" in href:
                file_url.append(str("https://www.sportsbookreviewsonline.com/scoresoddsarchives/mlb/" + href))


        for x in file_url:

            year = x.split("%")[-1].split(".")[0][2:]
            path_save = self.paths[3] + "/MLB_Odds_" + str(year) + ".csv"
            if not os.path.exists(path_save) or year == str(datetime.now().year):

                file = pd.read_excel(x)
                file.to_csv(path_save, index = False)
                print("Downloaded:" + "\t" + path_save)




        FG_teams = []
        FG_teams = np.array(FG_teams)
        all_teams = np.array(list(set(list(FG_teams))))
        team_index = []
        for teams in all_teams:
            team_index.append(np.where(FG_teams == teams)[0])


        #Format the files
        frame = []

        for i in tqdm(range(2010, (datetime.now().year + 1))):

            path_check = self.paths[3] + "/MLB_Odds_" + str(i) + ".csv"
            if os.path.exists(path_check):

                temp = pd.read_csv(path_check).reset_index(drop = True)
                temp.columns = temp.columns.str.replace(" ", "")

                #Fix dates
                temp["Date"] = temp["Date"].astype(str)

                for j in range(0, len(temp)):
                    u = temp.at[j, "Date"]
                    if len(temp.at[j, "Date"]) == 3:
                        temp.at[j, "Date"] = str(i) + "-" + "0" + u[0] + "-" + u[1:]
                    else:
                        temp.at[j, "Date"] = str(i) + "-" + u[0:2] + "-" + u[2:]


                #Convert moneyline values to returns
                moneylines = ["Open", "Close"]

                rmv = np.where(np.logical_or(temp["Open"].astype(str) == "NL", temp["Final"].astype(str) == "NL"))[0]
                if len(rmv) > 0:
                    temp = temp.drop(rmv)
                    temp = temp.reset_index(drop = True)


                #Fix missing column names
                #Older files to not have Run Lines
                to_fix = np.array([i for i,x in enumerate(temp.columns) if "Unnamed" in x])
                if len(to_fix) == 2:

                    new_cols = np.array(temp.columns)
                    new_cols[to_fix] = ["OU_ML_Open", "OU_ML_Close"]
                    temp.columns = new_cols

                    col_index = ["Date", "Team", 
                                "Open", "Close", "OpenOU", "CloseOU",
                                "OU_ML_Open", "OU_ML_Close",
                                "VH", "Final", "Pitcher"]

                elif len(to_fix) == 3:

                    new_cols = np.array(temp.columns)
                    new_cols[to_fix] = ["RunLine_ML", "OU_ML_Open", "OU_ML_Close"]  
                    temp.columns = new_cols 

                    col_index = ["Date", "Team", 
                                "Open", "Close", "OpenOU", "CloseOU", "RunLine",
                                "OU_ML_Open", "OU_ML_Close", "RunLine_ML",
                                "VH", "Final", "Pitcher"]


                split_frames = []
                values = ["H", "V"]
                temp = temp[col_index]

                str_cols = ["Date", "Team", "VH", "Pitcher"]
                numeric_cols = [x for x in temp.columns if x not in str_cols]

                #Format the numeric variables
                temp[numeric_cols] = temp[numeric_cols].astype(str)
                for col in numeric_cols:

                    temp.loc[:, col] = pd.to_numeric(temp.loc[:, col].str.replace("½",".5"))

                    #Format moneylines
                    if col in ["Open", "Close"] or "ML" in col:

                        under = np.where(temp[col] < 0)[0]
                        over = np.where(temp[col] >= 0)[0]

                        temp.loc[under, col] = -100 / temp.loc[under, col]
                        temp.loc[over, col] = temp.loc[over, col] / 100                     



                temp["Pitcher"] = temp["Pitcher"].str.replace("-L", "").str.replace("-R", "")
                for j in range(0, len(temp)):
                    temp.at[j, "Pitcher"] = str(temp.at[j, "Pitcher"])[1:]

                #Translate team names
                temp = self.Fix_Team_Names(temp, "City")
                temp = temp.reset_index(drop = True)


                for vals in values:

                    index = np.where(temp["VH"] == vals)[0]
                    split_frames.append(temp.iloc[index, :])
                    del split_frames[-1]["VH"]

                    cols = [x for x in list(temp.columns) if x != "VH"]

                    if vals == "H":
                        split_frames[-1].columns = [x + "_Home" if x != "Date" else "Date" for x in cols]
                    else:
                        split_frames[-1].columns = [x + "_Away" if x != "Date" else "Date" for x in cols]
                        del split_frames[-1]["Date"]

                    split_frames[-1] = split_frames[-1].reset_index(drop = True)

                #Assemble
                temp = pd.concat(split_frames, axis = 1, sort = True)

                #Compute implied odds
                temp["Open_Winning_Odds_Home"] = 1 / (1 + temp["Open_Home"])
                temp["Close_Winning_Odds_Home"] = 1 / (1 + temp["Close_Home"])

                temp["Open_Winning_Odds_Away"] = 1 / (1 + temp["Open_Away"])
                temp["Close_Winning_Odds_Away"] = 1 / (1 + temp["Close_Away"])

                temp["Open_Winning_Odds_Home"] = temp["Open_Winning_Odds_Home"] / (temp["Open_Winning_Odds_Home"] + temp["Open_Winning_Odds_Away"])
                temp["Close_Winning_Odds_Home"] = temp["Close_Winning_Odds_Home"] / (temp["Close_Winning_Odds_Home"] + temp["Close_Winning_Odds_Away"])

                temp["Open_Winning_Odds_Away"] = 1 - temp["Open_Winning_Odds_Home"]
                temp["Close_Winning_Odds_Away"] = 1 - temp["Close_Winning_Odds_Home"]

                #Fix score column names
                fix = np.array([i for i,x in enumerate(temp.columns) if "Final" in x])
                new_cols = np.array(list(temp.columns))
                new_cols[fix] = ["Score_Home", "Score_Away"]
                temp.columns = new_cols

                if len(frame) == 0:
                    frame = temp
                else:
                    frame = frame.append(temp)


        frame = frame.iloc[::-1]
        frame = frame.reset_index(drop = True)
        frame = self.Fix_Team_Names(frame, "City")  



        path_scores = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
        if os.path.exists(path_scores):

            print("\t" + "\t" + "\t" + "***** Adding IDs *****")

            scores = pd.read_csv(path_scores)

            #Attempt to add IDs
            scores["Win"] = 1
            scores.loc[np.where(scores["Score_Home"] < scores["Score_Away"])[0], "Win"] = 0

            frame["Win"] = 1
            frame.loc[np.where(frame["Score_Home"] < frame["Score_Away"])[0], "Win"] = 0    

            to_join = scores[["Date", "Team_Home", "Team_Away", "ID", "Win"]].copy().drop_duplicates(["Team_Home", "Team_Away", "Date"], keep = "first").reset_index(drop = True)
            frame = pd.merge(to_join, frame, on = ["Date", "Team_Home", "Team_Away", "Win"], how = "inner")


            frame.to_csv(self.paths[3] + "/Clean_Data/MLB_Odds.csv", index = False)
            print("\t" + "\t" + "***** MLB Moneyline data successfully formated *****")


    def Extract_Scores_per_Inning(self):

        #Check if the files have been scraped
        extract_dir = self.paths[2] + "/Play_by_play"
        if not os.path.exists(extract_dir):

            print("Error: no play-by-plays have been scraped yet.")
            print("Missing files at: " + extract_dir)



        #Clean
        files_in_dir = [f for f in listdir(extract_dir) if isfile(join(extract_dir, f))]
        if len(files_in_dir) == 0:

            print("Error: no play-by-plays have been scraped yet.")
            print("Missing files at: " + extract_dir)


        save_dir = self.paths[2] + "/Live_Scores"
        if not os.path.exists(save_dir):

            print("Creating directory at: " + save_dir)
            
            os.mkdir(save_dir)

        #Saving scores wrt innings...
        for file in tqdm(files_in_dir):

            frame = pd.read_csv(extract_dir + "/" + file)

            frame[["Score_Home_Live", "Score_Away_Live"]] = frame["Score"].str.split("-", expand = True)
            frame["Score_Home_Live"] = frame["Score_Home_Live"].astype(int)
            frame["Score_Away_Live"] = frame["Score_Away_Live"].astype(int)

            frame = frame.loc[:, ["Date", "ID", "Team_Home", "Team_Away", "Inn.", "Score_Home_Live", "Score_Away_Live", "URL"]].drop_duplicates().reset_index(drop = True)

            frame.to_csv(save_dir + "/" + file, index = False)                      


    ###########################################################
    #################### UPDATE CODES  ########################
    ###########################################################


    #MAIN FUNCTION
    #Scrapes within the interval [last_scrapped, today]
    #Update the Box_Scores
    #Clean the data if needed
    def UPDATE_FanGraphs_Box_Scores(self):

        path_check = self.paths[2] + "/FanGraphs_Scores.csv"
        if not os.path.exists(path_check):
            sys.exit("Missing file:" + "\t" + path_check)

        temp = pd.read_csv(path_check)
        n = len(temp)

        frm = temp["Date"].max()
        to = datetime.strftime(datetime.now(), "%Y-%m-%d")

        self.Get_FanGraphs_Game_URLs(frm, to)
        self.Extract_FanGraphs_Box_Scores()
        self.Extract_FanGraphs_Play_by_play()


        n_new = len(pd.read_csv(path_check))
        if n_new > n:
            print("Cleaning data...")
            self.Clean_Data()
            print("Scrapping lineups...")
            self.Scrape_BASEBALL_REFERENCE_lineups()
            print("Processing scores-per-inning frames...")
            #self.Extract_Scores_per_Inning()
            #print("Done.")
        else:
            print("No new Box Scores to scrape.")




    ##############################################################################
    #################### PREDICTED LINEUPS                 #######################
    ##############################################################################
    

    def Scrape_Historical_Predicted_Lineups_from_date(self, date):

        url = "https://rotogrinders.com/lineups/mlb?date=" + date + "&site=draftkings"
        html = requests.get(url).content
        soup = BeautifulSoup(html, features="lxml")

        tables = soup.find_all("div", {"class" : "lineup-content"})
        teams = [x.text for x in soup.find_all("span", {"class" : "shrt"})]

        bat = pd.DataFrame(columns = ["Date", "Name", "Team"])
        pitch = pd.DataFrame(columns = ["Date", "Name", "Team"])
        moneylines = pd.DataFrame(columns = ["Date", "Team_Home", "Factor_Home_Model", "Team_Away", "Factor_Away_Model"])

        lines = soup.find_all("div", {"class" : "ou"})


        i = 0
        j = 0
        for t in tables:

            try:

                names = t.find_all("a", {"class" : "player-popup"})
                positions = t.find_all("span", {"class" : "position"})

                container = pd.DataFrame(columns = ["Date", "Name", "Team"],
                                            index = np.arange(0, len(names)))

                m = int(len(container) / 2)

                container["Name"] = [x.text.strip() for x in names]
                container["Date"] = date

                container.loc[0:m, "Team"] = teams[i]
                container.loc[m:, "Team"] = teams[i+1]

                i += 2

                bat = bat.append(container.loc[1:(m-1), :])
                bat = bat.append(container.loc[m+1:len(container), :])

                pitch = pitch.append(container.loc[[0, m], :])

                #Fill the moneylines
                factors = lines[j].find_all("div")

                container_2 = pd.DataFrame(columns = moneylines.columns,
                                            index = np.arange(0, 1))

                container_2.at[0, "Team_Home"] = teams[i+1]
                container_2.at[0, "Team_Away"] = teams[i]

                container_2.at[0, "Date"] = date

                try:

                    f = [float(factors[-1].text.split("(")[-1].split(")")[0]), float(factors[0].text.split("(")[-1].split(")")[0])]
                    
                    for a in range(0, len(f)):
                        if f[a] > 0:
                            f[a] = np.round(1 + f[a] / 100, 2)
                        else:
                            f[a] = np.round(1 - 100 / f[a], 2)



                    container_2.at[0, "Factor_Home_Model"] = f[0]
                    container_2.at[0, "Factor_Away_Model"] = f[1]



                except:

                    container_2.at[0, "Factor_Home_Model"] = 0.0
                    container_2.at[0, "Factor_Away_Model"] = 0.0


                j += 1
                                                            
                moneylines = moneylines.append(container_2)

            except:

                i += 2
                continue


        pitch = pitch.reset_index(drop = True)
        bat = bat.reset_index(drop = True)
        moneylines = moneylines.reset_index(drop = True)

        return [bat, pitch, moneylines]


    def Scrape_BASEBALL_REFERENCE_lineups(self):

        #Retrieve the score file
        path_check = self.paths[2] + "/Clean_Data/FanGraphs_Scores.csv"
        if not os.path.exists(path_check):
            sys.exit("Missing file:" + "\t" + path_check)

        scores = pd.read_csv(path_check) 
        years = list(set(list(scores["Date"].str.split("-").str[0].astype(int))))

        teams = pd.DataFrame(list(set(list(scores["Team_Home"]))), columns = ["Team"])
        teams = self.Fix_Team_Names(teams, "Abr")

        i = np.where(teams["Team"] == "CUB")[0]

        if len(i) > 0:

            i = i[0]
            teams.at[i, "Team"] = "CHC"

        #Player names
        #Match names with the original database
        path_check = self.paths[0] + "/Clean_Data/FanGraphs_Box_Scores.csv"
        if not os.path.exists(path_check):
            sys.exit("Missing file:" + "\t" + path_check)

        batters = pd.read_csv(path_check)["Name"]
        batters = np.array(list(set(list(batters))))


        path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
        if not os.path.exists(path_check):
            sys.exit("Missing file:" + "\t" + path_check)

        pitchers = pd.read_csv(path_check)["Name"]
        pitchers = np.array(list(set(list(pitchers))))

        
        #----------------------------------------   
        #Prevents scrapping previous years twice
        years_done = []

        path_check = self.paths[1] + "/Clean_Data/Lineups_BR.csv"
        if os.path.exists(path_check):
            
            years_done = [x for x in list(set(list(pd.read_csv(path_check)["Date"].str.split("-").str[0].astype(int)))) if x != datetime.now().year]

        #----------------------------------------


        #Containers for saved frames    
        bat = []
        pitch = []


        #Scrape
        for year in years:

            if year in years_done:
                continue

            print("Scrapping season: " + str(year) + "...")

            for team in tqdm(teams["Team"]):

                #Get the starting lineup for batters
                url = "https://www.baseball-reference.com/teams/" + str(team) + "/" + str(year) + "-batting-orders.shtml"
                html = requests.get(url).content
                soup = BeautifulSoup(html, "lxml")

                #These are table rows with the player names and their position
                #Full names must be extracted (so no extract tables w/ pandas)
                rows = soup.find_all("tr", {"class" : ["R", "L"]})

                #Frame to append
                bat_cols = ["Name", "Position", "Number", "Date", "Team_Home", "Team_Away", "Score_Home", "Score_Away"]
                n_matches = len(rows)
                bat_index = list(range(0, 9*n_matches))

                bat_temp = pd.DataFrame(columns = bat_cols, index = bat_index) 

                i = 0
                for row in rows:

                    players = row.find_all("td")
                    info = row.find("th")

                    frm = 9*i
                    to = 9*(i + 1) - 1

                    names = []
                    positions = []
                    #Fill the kth row with players
                    for player in players:

                        name = str(player).split("title=")[1].split(">")[0].replace('"', '')
                        names.append(name)

                        position = player.text.split("-")[-1]
                        positions.append(position)

                    #Extract game info
                    #Will fix home / away later when merging to obtain IDs

                    date = ''.join([x for x in info.find_all("a")[1]["href"] if x.isdigit()])
                    date = date[0:4] + "-" + date[4:6] + "-" + date[6:8]

                    opponent = info.find_all("a")[-1].text

                    points = info.text.split("(")[1].replace(")", "").replace("#", "").split("-")
                    points = [int(x) for x in points]

                    bat_temp.loc[frm:to, "Name"] = names
                    bat_temp.loc[frm:to, "Position"] = positions
                    bat_temp.loc[frm:to, "Number"] = list(range(1,10))


                    bat_temp.loc[frm:to, "Date"] = date
                    bat_temp.loc[frm:to, "Team_Home"] = team
                    bat_temp.loc[frm:to, "Team_Away"] = info.find_all("a")[-1].text
                    bat_temp.loc[frm:to, "Score_Home"] = points[0]
                    bat_temp.loc[frm:to, "Score_Away"] = points[1]  

                    i += 1          

                #Fix team and player names
                bat_temp = self.Fix_Team_Names(bat_temp, "City")
                city = self.Translate_Team_Names(team, "City")
                for i in range(0, len(bat_temp)):
                    bat_temp.at[i, "Name"] = self.find_name(bat_temp.at[i, "Name"], city, batters)

                bat_temp = bat_temp.loc[np.where(bat_temp["Name"] != "None")[0]].reset_index(drop = True)


                #Add Ids
                first_merge = pd.merge(bat_temp, scores)

                temp = bat_temp.copy()
                bat_temp["Team_Home"] = temp["Team_Away"]
                bat_temp["Team_Away"] = temp["Team_Home"]
                bat_temp["Score_Home"] = temp["Score_Away"]
                bat_temp["Score_Away"] = temp["Score_Home"]

                second_merge = pd.merge(bat_temp, scores)

                merge = pd.concat([first_merge, second_merge]).sort_values(by = ["Date", "ID"])
                merge["Team"] = city

                #Append to bat
                if len(bat) == 0:
                    bat = merge
                else:
                    bat = bat.append(merge, ignore_index = True, sort = True)




                #Pitchers
                url = "https://www.baseball-reference.com/teams/tgl.cgi?team=" + str(team) + "&t=p&year=" + str(year)
                html = requests.get(url).content
                soup = BeautifulSoup(html, "lxml")


                rows = soup.find_all("tr")
                rows = [x for x in rows if x.get("id") is not None]

                n_matches = len(rows)


                #Frame to append
                pitch_cols = ["Name", "Umpire", "Date", "Team_Home", "Team_Away", "Score_Home", "Score_Away"]
                n_matches = len(rows)
                pitch_index = list(range(0, n_matches))

                pitch_temp = pd.DataFrame(columns = pitch_cols, index = pitch_index) 

                i = 0
                for row in rows:

                    points = [int(x) for x in row.find("td", {"data-stat" : "game_result"}).text.split(",")[-1].replace("#", "").split("-")]
                    
                    home =  row.find("td", {"data-stat" : "team_homeORaway"}).text == ""
                    opponent = row.find("td", {"data-stat" : "opp_ID"}).text

                    pitch_temp.at[i, "Name"] = row.find("td", {"data-stat" : "pitchers_number_desc"}).text.split(" (")[0]
                    pitch_temp.at[i, "Umpire"] = row.find("td", {"data-stat" : "umpire_hp"}).text
                    pitch_temp.at[i, "Date"] = row.find("td", {"data-stat" : "date_game"}).get("csk").split(".")[0]

                    if home:

                        pitch_temp.at[i, "Team_Home"] = team
                        pitch_temp.at[i, "Team_Away"] = opponent
                        pitch_temp.at[i, "Score_Home"] = points[0]
                        pitch_temp.at[i, "Score_Away"] = points[1]

                    else:

                        pitch_temp.at[i, "Team_Home"] = opponent
                        pitch_temp.at[i, "Team_Away"] = team
                        pitch_temp.at[i, "Score_Home"] = points[1]
                        pitch_temp.at[i, "Score_Away"] = points[0]


                    i += 1


                #Fix team name and player names
                pitch_temp = self.Fix_Team_Names(pitch_temp, "City")

                pitcher_names = [x for x in pitchers if city in x]
                for i in range(0, len(pitch_temp)):

                    family_name = pitch_temp.at[i, "Name"].split(".")[-1]
                    matches = [i for i, x in enumerate(pitcher_names) if family_name in x]

                    for k in matches:
                        if(pitcher_names[k][0] == pitch_temp.at[i, "Name"][0]):

                            pitch_temp.at[i, "Name"] = pitcher_names[k]
                            break

                #Merge with scores
                pitch_temp = pd.merge(pitch_temp, scores)
                pitch_temp["Team"] = city

                #Append to pitch
                if len(pitch) == 0:
                    pitch = pitch_temp
                else:
                    pitch = pitch.append(pitch_temp, ignore_index = True, sort = True)          


            #update and flush once we cycle through an entire year
            self.update_file(self.paths[0] + "/Clean_Data", "Lineups_BR.csv", bat)
            self.update_file(self.paths[1] + "/Clean_Data", "Lineups_BR.csv", pitch)

            bat = []
            pitch = []



    ##############################################################################
    #################### AVAIBLE BETS                      #######################
    ##############################################################################


    def Scrape_Bets(self):

        try:
            driver = webdriver.Chrome()
        except:
            print("Error: please install the proper Chrome webdriver at:")
            print(os.getcwd())
            
            #return 0


        # In[11]:


        driver.get("https://miseojeuplus.espacejeux.com/sports/sports/competition/597/matches")
        time.sleep(1.5)

        matches_containers = driver.find_elements_by_class_name("event-list__item-link")
        n_avb = len(matches_containers)


        # In[4]:


        #expand the match list
        try:
            more_matches = driver.find_element_by_class_name("content-loader__load-more-link")
            if str(type(more_matches)) == "<class 'selenium.webdriver.remote.webelement.WebElement'>":
                more_matches.click()

                matches_containers = driver.find_elements_by_class_name("event-list__item-link")

                #Keep loading until the new matches pop up
                while len(matches_containers) == n_avb:
                    time.sleep(1)
                    matches_containers = driver.find_elements_by_class_name("event-list__item-link")

        except:
            pass


        # In[5]:


        #html containers with the links to every match
        matches_containers = driver.find_elements_by_class_name("event-list__item-link")


        # In[6]:


        #get the time at which the matches are played
        times = []

        for containers in matches_containers:

            div_item = containers.find_element_by_class_name("event-card__event-time")
            match_time = div_item.get_attribute("innerHTML").split("</span>")[1].split(">")[-1].strip()
            times.append(match_time.replace("Aujourd'hui ", ""))

            
        #Process the game times
        for i in range(0, len(times)):

            try:
            
                gametime = np.NaN   

                if "Demain" in times[i]:

                    tmrw_time = times[i].split(" ")[-1].split(":")
                    gametime = datetime.now() + timedelta(days = 1)
                    gametime = gametime.replace(hour = int(tmrw_time[0]), minute = int(tmrw_time[1]), second = 0, microsecond = 0) 


                elif "Aujourd'hui" in times[i]:

                    tmrw_time = times[i].split(" ")[-1].split(":")
                    gametime = datetime.now() 
                    gametime = gametime.replace(hour = int(tmrw_time[0]), minute = int(tmrw_time[1]), second = 0, microsecond = 0) 
                    
                elif "s" in times[i] or "m" in times[i] or "h" in times[i]:

                    vals = times[i].split(" ")
                    h = 0
                    m = 0

                    for x in vals:

                        if "h" in x:
                            h += int(x.replace("h", ""))

                        elif "m" in x:
                            m += int(x.replace("m", ""))

                        elif "s" in x:
                            m += round(float(x.replace("s", "")) / 60.0)

                    gametime = datetime.now()
                    gametime += timedelta(hours = h, minutes = m)
                    gametime = gametime.replace(second = 0, microsecond = 0)         
                    

                elif ":" in times[i]:

                    tmrw_time = times[i].split(":")
                    gametime = datetime.now() 
                    gametime = gametime.replace(hour = int(tmrw_time[0]), minute = int(tmrw_time[1]), second = 0, microsecond = 0) 
     

                times[i] = gametime

            except:

                continue


        # In[32]:


        #get the urls for the individual match webpages
        match_urls = []
        for containers in matches_containers:
            match_urls.append(containers.find_element_by_class_name("event-list__item-link-anchor").get_attribute("href"))


        # In[33]:


        #process all pages
        scrapped_at = datetime.now().replace(second = 0, microsecond = 0)

        frames = []

        print("Retrieving avaible bets...")
        estimated_w_time = round((20.0 * len(matches_containers)) / 60)
        print("Estimated processing time: " + str(estimated_w_time) + "min(s)")

        to_remove = []
        k = -1
        for url in tqdm(match_urls):

            k += 1

            if type(times[k]) == str:
                to_remove.append(k)
                continue

            
            driver.get(url)
            
            #html containers with offered returns
            bet_containers = driver.find_elements_by_class_name("event-panel")
            
            #Wait for the page to load
            n_loads = 0
            skip = False
            while len(bet_containers) == 0:
                time.sleep(1)
                n_loads += 1
                bet_containers = driver.find_elements_by_class_name("event-panel")
                
                if n_loads == 10:
                    skip = True
                    print("Skipping unavaible event.")
                    print("URL: " + url)
                    break
                    
            if skip:
                to_remove.append(k)
                continue

            bet_frames = []

            #extract team names, rates and bet type
            for containers in bet_containers:

                try:
                    #Extract bets offered
                    offers = containers.find_elements_by_class_name("market__body_col")
                    rows = []

                    bet_type = containers.find_element_by_class_name("event-panel__heading").text

                    for x in offers:

                        x_separated = x.text.rsplit("\n", 1)  
                        if len(x_separated) == 1:
                            continue           

                        rows.append([bet_type, x_separated[0], x_separated[1]])

                    #Merge to main frame
                    to_append = pd.DataFrame(rows, columns = ["Bet_Type", "Bet_On", "Factor"])
                    if len(bet_frames) == 0:
                        bet_frames = to_append
                    else:
                        bet_frames = bet_frames.append(to_append, ignore_index = True)
                
                except:
                    continue
            
            #Expand menu
            expand_links = driver.find_elements_by_class_name("event-panel__heading__market-name")
            for link in expand_links:
                try:
                    link.click() 
                    
                except:
                    continue
            
            #To be safe
            time.sleep(1)
                
            #repeat process
            bet_containers = driver.find_elements_by_class_name("event-panel")

            #extract team names, rates and bet type
            for containers in bet_containers:

                try:
                    #Extract bets offered
                    offers = containers.find_elements_by_class_name("market__body_col")
                    rows = []

                    bet_type = containers.find_element_by_class_name("event-panel__heading").text

                    for x in offers:

                        x_separated = x.text.rsplit("\n", 1)  
                        if len(x_separated) == 1:
                            continue

                        rows.append([bet_type, x_separated[0], x_separated[1]])

                    #Merge to main frame
                    to_append = pd.DataFrame(rows, columns = ["Bet_Type", "Bet_On", "Factor"])
                    if len(bet_frames) == 0:
                        bet_frames = to_append
                    else:
                        bet_frames = bet_frames.append(to_append, ignore_index = True)   
                
                except:
                    continue
                    
            
            #Add the date and time
            bet_frames["Scrapping_Time"] = scrapped_at
            
            frames.append(bet_frames)
            
            


        # In[34]:


        #Remove unprocessed data urls and gametimes

        if len(to_remove) > 0:
            
            to_remove.reverse()
            for i in to_remove:
                
                del times[i]
                del match_urls[i]


        #Add referee names, game time and home/away team indicators
        for i in range(0, len(times)):
            
            refs_and_teams = match_urls[i].rsplit("/", 1)[-1].split("--")
            frames[i].loc[:, "Bet_Type"] = frames[i]["Bet_Type"].str.upper()
            
            teams = list(frames[i].loc[np.where(frames[0]["Bet_Type"] == "GAGNANT À 2 ISSUES")[0]]["Bet_On"])
            teams = [x.replace("(", "").replace(")", "") for x in teams]
            teams = [x.replace("Philadelphie", "Philadelphia") for x in teams]
            
            refs = []
            
            for j in range(0, len(refs_and_teams)):
                refs.append("".join(refs_and_teams[j].rsplit("-", 2)[1:])) 


            frames[i]["Game_Time"] = times[i]
            
            frames[i]["Team_Home"] = teams[-1]
            frames[i]["Team_Away"] = teams[0]
            
            frames[i]["Ref_Home"] = refs[-1]
            frames[i]["Ref_Away"] = refs[0]
            
            frames[i].loc[:, "Factor"] = pd.to_numeric(frames[i]["Factor"].str.replace(",", "."))
            
                      


        # In[35]:



        print("Cleaning LotoQc data...")

        # In[36]:


        #More data cleaning
        for i in range(0, len(frames)):

            #Fix the fucking annoying PHILLY bug
            bugged = [index for index, x in enumerate(list(frames[i]["Bet_On"])) if "PHILLIES" in x.upper()]
            if len(bugged) > 0:

                if frames[i].at[0, "Team_Home"].upper() == "PHI":
                    frames[i].loc[bugged, "Bet_On2"] = "Home"

                else:
                    frames[i].loc[bugged, "Bet_On2"] = "Away"
            
            frames[i].loc[:, "Bet_On"] = frames[i]["Bet_On"].str.replace("(", "", regex=True).str.replace(")", "", regex=True)
            frames[i].loc[:, "Bet_Type"] = frames[i]["Bet_Type"].str.replace("(", "", regex=True).str.replace(")", "", regex=True)

            frames[i]["Bet_On2"] = "None"
            frames[i]["Bet_Spread"] = 0.0

            #Add a column indicating if you are betting on the home, or visiting team
            for j in range(0, len(frames[i])):

                if frames[i].at[j, "Team_Home"] in frames[i].at[j, "Bet_On"] or frames[i].at[j, "Team_Home"] in frames[i].at[j, "Bet_Type"]:
                    frames[i].at[j, "Bet_On2"] = "Home"

                elif frames[i].at[j, "Team_Away"] in frames[i].at[j, "Bet_On"] or frames[i].at[j, "Team_Away"] in frames[i].at[j, "Bet_Type"]:
                    frames[i].at[j, "Bet_On2"] = "Away"

                if "\n" in frames[i].at[j, "Bet_On"]:
                    frames[i].at[j, "Bet_Spread"] = float(frames[i].at[j, "Bet_On"].split("\n")[-1])

                if "Moins de" in frames[i].at[j, "Bet_On"]:
                    frames[i].at[j, "Bet_Spread"] *= -1

                if "Gagnant par" in frames[i].at[j, "Bet_On"] and not "+" in frames[i].at[j, "Bet_On"]:
                    frames[i].at[j, "Bet_Spread"] = float(frames[i].at[j, "Bet_On"].split("par")[-1].split("point")[0].replace("s", ""))
            
            #Fix bug because loto-qc changed their website
            possibly_bugged = [index for index, x in enumerate(list(frames[i]["Bet_On2"])) if x == "None"]
            if len(possibly_bugged) > 0:

                for j in possibly_bugged:

                    if frames[i].at[j, "Team_Home"].upper() in frames[i].at[j, "Bet_Type"]:

                        frames[i].at[j, "Bet_On2"] = "Home"

                    elif frames[i].at[j, "Team_Away"].upper() in frames[i].at[j, "Bet_Type"]:

                        frames[i].at[j, "Bet_On2"] = "Away"




            


        # In[37]:


        #Fix team names
        for i in range(0, len(frames)):
            frames[i] = self.Fix_Team_Names(frames[i], "City")
            

        #Check if the game has already started
        for i in range(0, len(frames)):

            if str(type(frames[i].at[0, "Game_Time"])) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                frames[i]["Game_Started"] = False
                frames[i]["Minutes_Until_Start"] = frames[i]["Game_Time"] - frames[i]["Scrapping_Time"]
            else:
                frames[i]["Game_Started"] = True
                frames[i]["Minutes_Until_Start"] = timedelta(days = 0)


        # In[38]:


        #Unlist frames
        final_frame = pd.concat(frames, axis = 0, ignore_index = True)

        #Remove accents
        final_frame.loc[:, "Bet_Type"] = final_frame["Bet_Type"].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

        #Remove spacing, i.e.: the "\n"
        final_frame.loc[:, "Bet_On"] = final_frame["Bet_On"].str.replace("\n", " ")

        #Holy fucking shit, need to fix the PHILLY bug again??? 
        fucking_bitch = [index for index, x in enumerate(list(final_frame["Bet_Type"])) if "PHILLIES" in x]
        if len(fucking_bitch) > 0:

            if final_frame.at[fucking_bitch[0], "Team_Home"] == "PHI":

                final_frame.loc[fucking_bitch, "Bet_On2"] = "Home"

            else:

                final_frame.loc[fucking_bitch, "Bet_On2"] = "Away"


        # In[39]:


        #save
        folder_path = self.paths[3] + "/Predicted_Lineups/" + datetime.now().strftime("%d-%m-%Y")
        if not os.path.exists(folder_path):
            
            print("Creating directory at:")
            print(folder_path)
            
            os.mkdir(folder_path)
            
        print("Saving...")
        self.update_file(folder_path, "Bets.csv", final_frame)

        print("Done.")


        # In[40]:


        #obtain lineups
        print("Obtaining lineups...")
        date = datetime.strftime(datetime.now(), "%Y-%m-%d")

        lineups = self.Scrape_Historical_Predicted_Lineups_from_date(date)[0:2]

        #Fix team names
        for i in range(0, len(lineups)):
            lineups[i] = self.Fix_Team_Names(lineups[i], "City")


        # In[41]:


        #Fix player names
        print("Fixing player names...")

        path_check = self.paths[0] + "/Clean_Data/FanGraphs_Box_Scores.csv"
        if not os.path.exists(path_check):
            sys.exit("Missing file:" + "\t" + path_check)

        batters = list(set(list(pd.read_csv(path_check)["Name"])))

        path_check = self.paths[1] + "/Clean_Data/FanGraphs_Box_Scores.csv"
        if not os.path.exists(path_check):
            sys.exit("Missing file:" + "\t" + path_check)

        pitchers = list(set(list(pd.read_csv(path_check)["Name"]))) 

        bat = lineups[0]
        pitch = lineups[1]


        #Fix player names
        for i in range(0, len(bat)):
            bat.at[i, "Name"] = self.find_name(str(bat.at[i, "Name"]), str(bat.at[i, "Team"]), batters)

            if i < len(pitch):      
                pitch.at[i, "Name"] = self.find_name(str(pitch.at[i, "Name"]), str(pitch.at[i, "Team"]), pitchers)

                
            


        # In[42]:


        #Save the lineups
        print("Saving lineups...")

        self.update_file(folder_path, "Bat.csv", bat)
        self.update_file(folder_path, "Pitch.csv", pitch)

        print("Done with Loto Qc...")
        print("Scrapping Pinnacle...")


        url = "https://www.pinnacle.com/en/baseball/mlb/matchups"
        driver.get(url)


        # In[196]:


        bets_url = driver.find_elements_by_css_selector("[data-test-id='Event.MarketCnt']")

        #Wait for item to load
        ticks = 0
        while len(bets_url) == 0 and ticks <= 4:
            time.sleep(1)
            ticks += 1
            bets_url = driver.find_elements_by_css_selector("[data-test-id='Event.MarketCnt']")

        bets_url = [x.get_attribute("href") for x in bets_url]

        if len(bets_url) == 0:

            bets_url = driver.find_elements_by_css_selector("[class^='style_metadata']")
            bets_url = [x.find_elements_by_css_selector("*")[0].get_attribute("href") for x in bets_url]

        scrapping_time = scrapped_at

        # In[197]:


        all_frames = []

        for u in tqdm(bets_url):

            driver.get(u)
            expand_buttons = driver.find_elements_by_css_selector("span[class^='style_toggleMarkets']")

            #Wait for item to load
            ticks = 0
            while len(expand_buttons) == 0 and ticks <= 4:
                time.sleep(1)
                ticks += 1
                expand_buttons = driver.find_elements_by_css_selector("span[class^='style_toggleMarkets']")

            if len(expand_buttons) > 0:
                for b in expand_buttons:
                    b.click()

            time.sleep(1)

            tables = driver.find_elements_by_css_selector("div[data-collapsed='false']")
            all_bets = []

            time.sleep(1)

            try:
                match_date = parser.parse(driver.find_element_by_css_selector("div[class^='style_startTime']").text)
                time_diff = np.timedelta64((match_date - scrapping_time), "m") / np.timedelta64(1, 'm')
                if time_diff >= 12*60:
                    continue

            except:
                continue


            for t in tables:
                try:
                    title = t.find_element_by_css_selector("span[class^='style_titleText']").text
                    rows = t.find_elements_by_css_selector("div[class^='style_buttonRow']")

                except:
                    continue
                
                try:
                    additional_info = t.find_element_by_css_selector("ul[class^='style_subHeading']")
                    additional_info = [x.text for x in additional_info.find_elements_by_css_selector("li")]

                except:
                    additional_info = [np.NaN, np.NaN]


                nrow = len(rows)
                rcount = 0
                rcount_cutoff = len(rows) / 2

                for r in rows:
                    bets = r.find_elements_by_css_selector("button")
                    cutoff = len(bets) / 2

                    count = 0
                    for b in bets:

                        try:

                            bet_on = b.find_element_by_css_selector("span[class^='style_label']").text
                            f = b.find_element_by_css_selector("span[class^='style_price']").text

                            if not "Team Total" in title: 

                                if count < cutoff:
                                    all_bets.append([title, 9, bet_on, additional_info[0], f])

                                else:
                                    all_bets.append([title, 9, bet_on, additional_info[1], f])
                                    
                                count += 1

                            else:

                                if rcount < rcount_cutoff:
                                    all_bets.append([title, 9, bet_on, additional_info[0], f])

                                else:
                                    all_bets.append([title, 9, bet_on, additional_info[1], f])
                                    
                                count += 1


                        except:

                            count += 1

                    rcount += 1

            try:
            
                half_match_button = driver.find_element_by_css_selector("button[id='period:1']")
                time.sleep(1)
                half_match_button.click()
                                
                #Repeat, for half-matches
                expand_buttons = driver.find_elements_by_css_selector("span[class^='style_toggleMarkets']")

                #Wait for item to load
                ticks = 0
                while len(expand_buttons) == 0 and ticks <= 4:
                    time.sleep(1)
                    ticks += 1
                    expand_buttons = driver.find_elements_by_css_selector("span[class^='style_toggleMarkets']")


                for b in expand_buttons:
                    b.click()

                time.sleep(1)

                tables = driver.find_elements_by_css_selector("div[data-collapsed='false']")

                for t in tables:
                    title = t.find_element_by_css_selector("span[class^='style_titleText']").text
                    rows = t.find_elements_by_css_selector("div[class^='style_buttonRow']")

                    try:
                        additional_info = t.find_element_by_css_selector("ul[class^='style_subHeading']")
                        additional_info = [x.text for x in additional_info.find_elements_by_css_selector("li")]

                    except:
                        additional_info = [np.NaN, np.NaN]

                    nrow = len(rows)
                    rcount = 0
                    rcount_cutoff = len(rows) / 2

                    for r in rows:
                        bets = r.find_elements_by_css_selector("button")
                        cutoff = len(bets) / 2

                        count = 0
                        for b in bets:

                            try:

                                bet_on = b.find_element_by_css_selector("span[class^='style_label']").text
                                f = b.find_element_by_css_selector("span[class^='style_price']").text

                                if not "Team Total" in title: 

                                    if count < cutoff:
                                        all_bets.append([title, 5, bet_on, additional_info[0], f])

                                    else:
                                        all_bets.append([title, 5, bet_on, additional_info[1], f])
                                        
                                    count += 1

                                else:

                                    if rcount < rcount_cutoff:
                                        all_bets.append([title, 5, bet_on, additional_info[0], f])

                                    else:
                                        all_bets.append([title, 5, bet_on, additional_info[1], f])
                                        
                                    count += 1


                            except:

                                count += 1

                        rcount += 1


            except:

                pass

            all_bets = pd.DataFrame(all_bets, columns = ["Bet_Type", "Inn.", "Bet_On", "Bet_On2", "Factor"])
            all_bets.loc[:, "Factor"] = all_bets["Factor"].astype(float)


            all_bets["Scrapping_Time"] = scrapping_time
            all_bets["Game_Time"] = match_date
            all_bets["Minutes_Until_Start"] = time_diff

            teams = [x.replace("-", " ").title() for x in u.split("/")[-2].split("-vs-")]

            all_bets["Team_Home"] = teams[1]
            all_bets["Team_Away"] = teams[0]

            all_bets.loc[:, "Bet_On"] = ["Home" if teams[1] in str(x) else "Away" if teams[0] in str(x) else x for x in list(all_bets["Bet_On"])]
            all_bets.loc[:, "Bet_On2"] = ["Home" if teams[1] in str(x) else "Away" if teams[0] in str(x) else "None" for x in list(all_bets["Bet_On2"])]

            index = np.where(np.logical_or(all_bets["Bet_On"] == "Home", all_bets["Bet_On"] == "Away"))[0]
            if len(index) > 0:
                all_bets.loc[index, "Bet_On2"] = all_bets.loc[index, "Bet_On"]

            all_bets = self.Fix_Team_Names(all_bets, "City")
            all_frames.append(all_bets)

            
        all_frames = pd.concat(all_frames)
        all_frames = all_frames.reset_index(drop = True)

        all_frames["Team_Home"] = [str(x) for x in all_frames["Team_Home"]]
        all_frames["Team_Away"] = [str(x) for x in all_frames["Team_Away"]]

        all_frames = all_frames.loc[np.where(np.logical_and(all_frames["Team_Home"] != "None", all_frames["Team_Away"] != "None"))[0]]
        all_frames = all_frames.reset_index(drop = True)


        #Formating
        def g(x):
            x = str(x)
            if "Under" in x or "Over" in x:
                return float(x.split(" ")[-1])
            else:

                try:
                    return float(x)
                except:
                    return 0.0

        #Remove bets causing formating errors
        rmv = [i for i,x in enumerate(list(all_frames["Bet_On"])) if "Errors" in str(x)]
        if len(rmv) > 0:

            all_frames = all_frames.drop(rmv).reset_index(drop = True)



        all_frames["Bet_Type2"] = [g(x) for x in list(all_frames["Bet_On"])]


        temp = list(all_frames["Bet_On"])
        temp2 = list(all_frames["Bet_On2"])

        for j in range(0, len(temp)):

            if "Under" in temp[j]:

                all_frames.at[j, "Bet_On"] = "Below"

            elif "Over" in temp[j]:

                all_frames.at[j, "Bet_On"] = "Above"

            elif temp2[j] == "Home":

                all_frames.at[j, "Bet_On"] = all_frames.at[j, "Team_Home"]

            elif temp2[j] == "Away":

                all_frames.at[j, "Bet_On"] = all_frames.at[j, "Team_Away"]

            else:

                all_frames.at[j, "Bet_On"] = "None"


        

        for j in range(0, len(all_frames)):

            if "Team Total" in all_frames.at[j, "Bet_Type"]:

                all_frames.at[j, "Bet_Type"] = "POINTS"

            elif "Total" in all_frames.at[j, "Bet_Type"]:

                all_frames.at[j, "Bet_Type"] = "SUM"

            elif "Handicap" in all_frames.at[j, "Bet_Type"]:

                all_frames.at[j, "Bet_Type"] = "SPREAD"

            elif "Moneyline" in all_frames.at[j, "Bet_Type"]:

                all_frames.at[j, "Bet_Type"] = "WINNER"


        all_frames.loc[:, "Date"] = all_frames["Game_Time"].dt.date


        folder_path = self.paths[3] + "/Predicted_Lineups/" + datetime.now().strftime("%d-%m-%Y")
        if not os.path.exists(folder_path):

            print("Creating directory at:")
            print(folder_path)

            os.mkdir(folder_path)

        print("Saving...")
        self.update_file(folder_path, "Bets_Pinnacle.csv", all_frames)


        def process_row(row):

            game_info = row.find_element_by_css_selector("[class^=compactBettingOptionContainer]")

            game_time = game_info.find_element_by_css_selector("[class^=timeContainer]")
            game_time = game_time.text.split("\n")[0]
            
            hour = int(game_time.split(":")[0])
            if "PM" in game_time and int(game_time[0:2]) != 12:
                hour += 12

            minutes = int(game_time.split(":")[-1].split(" ")[0])

            game_time = datetime.today().replace(hour = hour, minute = minutes, second = 0, microsecond = 0)

            if game_time < datetime.today():

                print("Game has already started.")
                return None

            participants = game_info.find_elements_by_css_selector("[class^=participantContainer]")
            participants = [x.text for x in participants]
            participants = [x.split("\n")[1] for x in participants]

            row = row.find_element_by_css_selector("[class^=consensusAndoddsContainer]")
            values = row.find_elements_by_css_selector("[class^=oddsNumber]")

            visitor = [x.text for i,x in enumerate(values) if i % 2 == 0]
            home = [x.text for i,x in enumerate(values) if i % 2 == 1]

            del visitor[0:2]
            del home[0:2]

            #visitor = [float(x) if x not in ["-", ""] else x for x in visitor]
            #home = [float(x) if x not in ["-", ""] else x for x in home]

            visitor = [participants[0]] + [game_time] + ["Away"] + visitor
            home = [participants[1]] + [game_time] + ["Home"] + home

            out = pd.DataFrame([visitor, home])

            return out


        print("Scrapping SBR...")

        urls = ["https://www.sportsbookreview.com/betting-odds/mlb-baseball/money-line/",
                "https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/",
                "https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/"]

        bet_types = ["WINNER",
                        "SUM",
                        "SPREAD"]

        frames = []

        for j in range(0, len(urls)):

            driver.get(urls[j])
            temporary_frames = []

            while True:

                rows = driver.find_elements_by_css_selector("[class^='eventMarketGridContainer']")
                while len(rows) == 0:
                    time.sleep(1)
                    rows = driver.find_elements_by_css_selector("[class^='eventMarketGridContainer']")


                previous_time = datetime.today()
                next_time = previous_time + timedelta(hours = 1)

                all_rows = []
                i = -1
                while next_time >= previous_time:

                    i += 1

                    if len(all_rows) > 0:
                        previous_time = all_rows[-1].at[0, 1]

                    try:
                        new_row = process_row(rows[i])
                    except:
                        rows = driver.find_elements_by_css_selector("[class^='eventMarketGridContainer']")
                        new_row = process_row(rows[i])

                    if str(type(new_row)) == "<class 'NoneType'>":
                        continue

                    else:
                        next_time = new_row.at[0, 1]
                        all_rows.append(new_row)

                del all_rows[-1]

                all_rows = pd.concat(all_rows, axis = 0)


                sources = driver.find_elements_by_css_selector("[class^='sportbook']")
                while len(sources) == 0:
                    time.sleep(1)
                    sources = driver.find_elements_by_css_selector("[class^='sportbook']")

                try:
                    sources = [x for x in sources if len(x.find_elements_by_css_selector("[rel^='nofollow']")) > 0]
                    sources = [x.find_element_by_css_selector("[rel^='nofollow']").get_attribute("href").split("/")[-1].split("-")[0] for x in sources]

                except:
                    sources = driver.find_elements_by_css_selector("[class^='sportbook']")
                    sources = [x for x in sources if len(x.find_elements_by_css_selector("[rel^='nofollow']")) > 0]
                    sources = [x.find_element_by_css_selector("[rel^='nofollow']").get_attribute("href").split("/")[-1].split("-")[0] for x in sources]
                    
                del sources[0]

                all_rows.columns = ["Team"] + ["Match_Time"] + ["Location"] + sources

                temporary_frames.append(all_rows)


                next_page_button = driver.find_elements_by_class_name("sbr-icon-chevron-right")
                if len(next_page_button) == 0:
                    break
                else:
                    next_page_button[0].click()
                    time.sleep(3)


            out = functools.reduce(lambda x, y: pd.merge(x, y), temporary_frames)
            out.insert(0, "Bet_Type", list(np.repeat(bet_types[j], np.shape(out)[0], axis = 0)))

            frames.append(out)


        all_lines = pd.concat(frames, axis = 0)
        all_lines = self.Fix_Team_Names(all_lines, "City")

        all_lines.insert(0, "Scrapping_Time", list(np.repeat(scrapped_at, np.shape(all_lines)[0], axis = 0)))
        all_lines.insert(0, "Date", list(np.repeat(scrapped_at.date(), np.shape(all_lines)[0], axis = 0)))

        to_format = [x for x in all_lines.columns if x not in ['Date', 'Scrapping_Time', 'Bet_Type', 'Team', 'Match_Time', 'Location']]
        for j in range(0, len(to_format)):
            all_lines.loc[:, to_format[j]] = all_lines[to_format[j]].str.replace("½", ".5")


        

        self.update_file(folder_path, "SBR_data.csv", all_lines)

        driver.quit()
        print("Done.")