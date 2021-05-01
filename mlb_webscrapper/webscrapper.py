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

		n_new = len(pd.read_csv(path_check))
		if n_new > n:
			self.Clean_Data()
		else:
			print("No new Box Scores to scrape.")



	#Extracts the box scores based off the URL list
	def Extract_FanGraphs_Box_Scores_FROM_MISSING_MATCHES(self):

		url_path = self.paths[-1] + "/Missing_Matches.csv"
		if os.path.exists(url_path):

			file_missing_urls = pd.read_csv(url_path)

			urls = list(set(list(file_missing_urls["URL"])))

			#Checks for existing Box_Scores
			path_to_check = self.paths[2] + "/FanGraphs_Scores.csv"
			if os.path.exists(path_to_check):
				urls_done = list(pd.read_csv(path_to_check).drop_duplicates()["URL"])

				urls = [x for x in urls if x not in urls_done]


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
					tables = self.Scrape_FanGraphs_game_stats_by_url(url)
				except:
					time.sleep(random.randint(5,10))
					continue

				bat = self.update_frame(bat, tables[0].append(tables[1]))
				pitch = self.update_frame(pitch, tables[2].append(tables[3]))
				scores = self.update_frame(scores, tables[4])

				print("\t" + "\t" + "\t" + "***** ADDED GAME *****")
				print(scores.iloc[-1,:])

				#print(scores)

				if count % 20 == 0 or url == urls[-1]:

					self.update_file(self.paths[0], "FanGraphs_Box_Scores.csv", bat)	
					bat = []

					self.update_file(self.paths[1], "FanGraphs_Box_Scores.csv", pitch)	
					pitch = []					

					self.update_file(self.paths[2], "FanGraphs_Scores.csv", scores)	
					scores = []

					print("\t" + "\t" + "\t" + "***** PROGRESS SAVED *****")

				if url != urls[-1]:
					time.sleep(random.randint(3, 7))



	##############################################################################
	#################### PREDICTED LINEUPS AND MONEYLINES  #######################
	##############################################################################
	

	def Scrape_Historical_Predicted_Lineups_from_date(self, date):

		url = "https://rotogrinders.com/lineups/mlb?date=" + date + "&site=draftkings"
		html = requests.get(url).content
		soup = BeautifulSoup(html)

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



	def Betting_Webscrape(self):

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")
		
		url = "https://miseojeu.lotoquebec.com/fr/offre-de-paris/baseball/mlb/matchs?idAct=11"

		headers = {
	    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
		}

		print("Accessing Loto-Quebec website...")

		#Obtain page data
		html = requests.get(url , stream = True, headers = headers).content
		try:
			tables = pd.read_html(html)
			soup = BeautifulSoup(html)
		except:
			print("Error: No bets found   ----   Too early, or no games today.")
			return 0

		#Obtain moneylines
		moneylines = [x for x in tables if len(x.columns) == 4]
		moneylines = [x for x in moneylines if "Baseball  MLB" in x.iloc[0,1]]

		billet = pd.DataFrame([moneylines[0].iloc[0,1], moneylines[0].iloc[0,2]]).T

		for x in moneylines[1:]:
			temp = pd.DataFrame([x.iloc[0,1], x.iloc[0,2]]).T

			if "pt(s)" in temp.iloc[0,0]:
				break
			
			else:
				billet = billet.append(temp, ignore_index = True)

		billet.columns = ["Home", "Away"]

		teams = []
		returns = []

		for j in range(0, 2):

			temp = billet.iloc[:,j]
			nm = temp.str.split("  ")

			t = []
			r = []
			for i in range(0, len(temp)):

				t.append(nm[i][2])
				r.append(float(nm[i][3].replace(",", ".")))


			teams.append(t)
			returns.append(r)


		out = pd.DataFrame([teams[1], returns[1], teams[0], returns[0]]).T
		out.columns = ["Team_Home", "Factor_Home", "Team_Away", "Factor_Away"]
		out["Date"] = str(datetime.now()).split(" ")[0]

		out = self.Fix_Team_Names(out, "City")


		#Obtain predicted lineups
		lineups = self.Scrape_Historical_Predicted_Lineups_from_date(date)

		#Fix team names
		for i in range(0, len(lineups)):
			lineups[i] = self.Fix_Team_Names(lineups[i], "City")

		#Fix player names
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
		moneylines = lineups[2]

		#Fix player names
		for i in range(0, len(bat)):
			bat.at[i, "Name"] = self.find_name(str(bat.at[i, "Name"]), str(bat.at[i, "Team"]), batters)

			if i < len(pitch):		
				pitch.at[i, "Name"] = self.find_name(str(pitch.at[i, "Name"]), str(pitch.at[i, "Team"]), pitchers)


		#Merge LotoQc and the moneylines
		del moneylines["Date"] 

		billet = pd.merge(out, moneylines, on = ["Team_Home", "Team_Away"], how = "left").dropna()
		billet = billet.drop_duplicates(["Team_Home", "Team_Away"], keep = "first").reset_index(drop = True)

		#Update or save
		file_names = ["LotoQc_Moneylines.csv", "LotoQc_Batters.csv", "LotoQc_Pitchers.csv"]
		file = [billet, bat, pitch]


		for i in range(0, len(file_names)):
			path_check = self.paths[3] + "/Predicted_Lineups/" + file_names[i]

			if not os.path.exists(path_check):
				file[i].to_csv(path_check, index = False)

				if(i == 0):
					print(file[i])

			else:

				if i == 0:
					old_data = pd.read_csv(path_check)
					new_data = file[i].append(old_data, sort = True).reset_index(drop = True)
					new_data = new_data.drop_duplicates(["Team_Home", "Team_Away", "Date"], keep = "first").reset_index(drop = True)

					new_data.to_csv(path_check, index = False)
					print(file[i])

				else:

					old_data = pd.read_csv(path_check)
					keep = np.where(old_data["Date"] != date)[0]

					if len(keep) == 0:
						file[i].to_csv(path_check, index = False)
					else:
						old_data = old_data.loc[keep, :]
						new_data = file[i].append(old_data, sort = True).reset_index(drop = True)
						new_data.to_csv(path_check, index = False)	


		#Add IDs
		all_files = []
		for x in file_names:
			all_files.append(pd.read_csv(self.paths[3] + "/Predicted_Lineups/" + x))

		all_files[0]["ID"] = np.arange(0, len(all_files[0]))
		sub_frame = all_files[0][["Date", "ID"]].copy()
		sub_frame2 = sub_frame.copy()

		sub_frame["Team"] = all_files[0].loc[:, "Team_Home"].copy()
		sub_frame2["Team"] = all_files[0].loc[:, "Team_Away"].copy()

		sub_frame = sub_frame.append(sub_frame2, sort = True).reset_index(drop = True)

		for i in range(1, len(all_files)):
			all_files[i] = all_files[i].merge(sub_frame, on = ["Date", "Team"], how = "left").dropna().reset_index(drop = True)
			all_files[i]["ID"] = all_files[i]["ID"].astype(int).copy()

		#Remove IDs with missing data
		bad_IDs = []
		for i in range(1, len(all_files)):
			rmv = np.where(all_files[i]["Name"] == "None")[0]
			if len(rmv) > 0:
				bad_IDs.append(list(set(list(all_files[i].loc[rmv, "ID"]))))

		if len(bad_IDs) > 0:

			bad_IDs = np.array(list(chain(*bad_IDs)))
			for i in range(0, len(all_files)):

				keep = [j for j in range(0, len(all_files[i])) if all_files[i].at[j, "ID"] not in bad_IDs]
				all_files[i] = all_files[i].loc[keep, :].copy().reset_index(drop = True)


		#Remove double-day matches from the bat and pitch files
		for ids in list(all_files[0]["ID"]):

			index = np.where(all_files[1]["ID"] == ids)[0]
			if len(index) > 9*2:
				index = index[int(len(index)/2):]
				all_files[1] = all_files[1].drop(index, axis = 0).reset_index(drop = True)

			index = np.where(all_files[2]["ID"] == ids)[0]
			if len(index) > 2:
				index = index[int(len(index)/2):]
				all_files[2] = all_files[2].drop(index, axis = 0).reset_index(drop = True)


		#Save
		file_names = ["LotoQc_Moneylines_Clean.csv", "LotoQc_Batters_Clean.csv", "LotoQc_Pitchers_Clean.csv"]
		for i in range(0, len(file_names)):
			all_files[i].to_csv(self.paths[3] + "/Predicted_Lineups/" + file_names[i], index = False)

		self.Webscrape_LotoQc2()



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


	def Webscrape_LotoQc2(self):

		date = datetime.strftime(datetime.now(), "%Y-%m-%d")
		
		url = "https://miseojeu.lotoquebec.com/fr/offre-de-paris/baseball/mlb/matchs?idAct=11"

		headers = {
		'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
		}

		print("Accessing Loto-Quebec website...")

		#Obtain page data
		html = requests.get(url , stream = True, headers = headers).content
		try:
			tables = pd.read_html(html)
			soup = BeautifulSoup(html)
		except:
			print("Error: No bets found   ----   Too early, or no games today.")

		moneylines = [x for x in tables if len(x.columns) == 4]
		moneylines = [x for x in moneylines if "Baseball  MLB" in x.iloc[0,1]]


		#Remove bets on pitchers, homeruns, ect.
		for i in range(0, len(moneylines)):
			
			moneylines[i].columns = ["A", "B", "C", "D"]
			
		moneylines = pd.concat(moneylines, axis = 0)
		moneylines = moneylines.reset_index(drop = True)
		moneylines = moneylines.astype(str)

		keep = np.where(moneylines["A"] != "nan")[0]
		moneylines = moneylines.loc[keep].reset_index(drop = True)

		spreads = [i for i, x in enumerate(moneylines["B"]) if "pt(s)" in x]
		win = list(range(0, spreads[0]))
		totals = [i for i, x in enumerate(moneylines["B"]) if "Plus de" in x][0:(3 * len(win))]

		spreads = moneylines.loc[spreads]
		win = moneylines.loc[win]
		totals = moneylines.loc[totals]

		teams = win[["B", "C"]]

		for i in range(0, len(teams)):
			for j in range(0, 2):
			
				teams.iloc[i,j] = teams.iloc[i,j].split("MLB")[1].split("Num")[0]
				teams.iloc[i,j] = "".join([x for x in teams.iloc[i,j] if not x.isdigit() and x != ","])
				teams.iloc[i,j] = teams.iloc[i,j].strip()

		win_away = win["B"].str.split("MLB", n = 1, expand = True).iloc[:, 1].str.split("Num", n = 1, expand = True).iloc[:, 0].str.strip().str.split("  ", n = 1, expand = True)
		win_home = win["C"].str.split("MLB", n = 1, expand = True).iloc[:, 1].str.split("Num", n = 1, expand = True).iloc[:, 0].str.strip().str.split("  ", n = 1, expand = True)




		win_away.columns = ["Team_Away", "Factor_Away"]
		win_home.columns = ["Team_Home", "Factor_Home"]
		win_frame = pd.concat([win_away, win_home], axis = 1)


		win_frame = self.Fix_Team_Names(win_frame, "City")


		spread_away = spreads["B"].str.split("MLB", n = 1, expand = True).iloc[:, 1].str.split("Num", n = 1, expand = True).iloc[:, 0].str.strip()
		spread_home = spreads["C"].str.split("MLB", n = 1, expand = True).iloc[:, 1].str.split("Num", n = 1, expand = True).iloc[:, 0].str.strip()

		all_spreads = [pd.DataFrame(spread_home), pd.DataFrame(spread_away)]

		k = 0

		for frame in all_spreads:
			
			t = []
			s = []
			f = []
			
			for i in range(0, len(frame)):
				t.append("".join([x for x in frame.iloc[i,0].split("pt(s)")[0] if not x.isdigit() and x not in ["-", "+", ","]]).strip())
				s.append(float(frame.iloc[i,0].split("pt(s)")[0].strip().split(" ")[-1].replace(",", ".")))
				f.append(float(frame.iloc[i,0].split("pt(s)")[-1].strip().replace(",", ".")))

			t = pd.DataFrame(t)
			s = pd.DataFrame(s)
			f = pd.DataFrame(f)

			all_spreads[k] = pd.concat([t,s,f], axis = 1)
			if k == 0:
				all_spreads[k].columns = ["Team_Home", "Spread_Home", "Factor_Home"]
			else:
				all_spreads[k].columns = ["Team_Away", "Spread_Away", "Factor_Away"]
			
			k += 1


		all_spreads = pd.concat(all_spreads, axis = 1)
		all_spreads.loc[:, "Team_Away"] = list(teams["B"]) + list(teams["B"])
		all_spreads.loc[:, "Team_Home"] = list(teams["C"]) + list(teams["C"])
			
		all_spreads = self.Fix_Team_Names(all_spreads, "City")

		totals_over = totals["B"].str.split("de ", expand = True).iloc[:, 1].str.split("Num", expand = True).iloc[:, 0].str.strip().str.split("  ", expand = True)
		totals_under = totals["C"].str.split("de ", expand = True).iloc[:, 1].str.split("Num", expand = True).iloc[:, 0].str.strip().str.split("  ", expand = True)

		totals = pd.concat([totals_over, totals_under], axis = 1)

		temp = frame.iloc[i,0].split("pt(s)")[0].strip().split(" ")

		totals.columns = ["Over", "Factor_Over", "Under", "Factor_Under"]

		totals["Team_Home"] = "-"
		totals["Team_Away"] = "-"

		totals = totals.reset_index(drop = True)

		for i in range(0, len(win_frame)):
			totals.loc[i, "Team_Home"] = teams.loc[i, "C"]
			totals.loc[i, "Team_Away"] = teams.loc[i, "B"]

			totals.loc[2*i + len(win_frame), "Team_Home"] = teams.loc[i, "C"]
			totals.loc[2*i + len(win_frame), "Team_Away"] = teams.loc[i, "B"]  

			totals.loc[2*i + len(win_frame) + 1, "Team_Home"] = teams.loc[i, "C"]
			totals.loc[2*i + len(win_frame) + 1, "Team_Away"] = teams.loc[i, "B"]


		totals = self.Fix_Team_Names(totals, "City")    
		win_frame["Date"] = date
		totals["Date"] = date
		all_spreads["Date"] = date

		win_frame.loc[:,"Factor_Away"] = win_frame.loc[:,"Factor_Away"].str.replace(",",".").astype(float)
		win_frame.loc[:,"Factor_Home"] = win_frame.loc[:,"Factor_Home"].str.replace(",",".").astype(float)

		totals.loc[:,"Factor_Over"] = totals.loc[:,"Factor_Over"].str.replace(",",".").astype(float)
		totals.loc[:,"Factor_Under"] = totals.loc[:,"Factor_Under"].str.replace(",",".").astype(float)
		totals.loc[:,"Over"] = totals.loc[:,"Over"].str.replace(",",".").astype(float)
		totals.loc[:,"Under"] = totals.loc[:,"Under"].str.replace(",",".").astype(float)


		self.update_file(self.paths[3] + "/Predicted_Lineups", "LotoQc_ML.csv", win_frame)
		self.update_file(self.paths[3] + "/Predicted_Lineups", "LotoQc_OU.csv", totals)
		self.update_file(self.paths[3] + "/Predicted_Lineups", "LotoQc_SP.csv", all_spreads)






