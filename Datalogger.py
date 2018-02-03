#!/usr/bin/env python
# -*- coding: utf-8 -*-

#-------------------------------------------------------------------------------
# Name:        Datalogger.py
#
# Author:      Stefan Dittforth
#
# Created:     10.10.2014
# Version history:
#
#   1.0 - initial version
#   2.0 - added YouLess Stromzaehler
#	3.0 - fixed to read new page layout, refactored
#	4.0 - added InfluxDB writer, refactored
#
#-------------------------------------------------------------------------------

import argparse
import os.path
import re
from bs4 import BeautifulSoup
import httplib
from mechanize import Browser
import datetime
import logging.handlers
import sys
import urllib2
import json
import requests
from influxdb import DataFrameClient
import pandas as pd
import numpy as np

def main():
	my_logger.info("========= Skript wurde gestartet ========================")
	args = parse_args()
	database = Database(host=cfg['DBhost'],
						port=cfg['DBport'],
						dbuser=cfg['DBuser'],
						dbuser_password=cfg['DBuserPassword'],
						dbname=cfg['DBname'],
						dbmeasurement=cfg['DBmeasurement'])

	# ---------- import from CSV file ----------
	if args.import_from != None:
		my_logger.info("Importiere aus Datei '{}'.".format(args.import_from))
		CSV_data = CSV_Data(args.import_from)
		data_from_CSV = CSV_data.import_data()
		database.write(data_from_CSV)
	# ---------- export to CSV file ----------
	if args.export_to != None:
		# TODO: develop export
		pass
	# ---------- default: read from URL's ----------
	if args.import_from == None and args.export_to == None:
		tecalor = Tecalor()
		youless = YouLess()
		data = pd.concat([tecalor.read_measurements(cfg['TecalorDataURLs']),
						  youless.read_measurements(cfg['YouLessMeters'])],
						  axis=1)
		data['Uhrzeit'] = pd.Timestamp(datetime.datetime.now())
		data.set_index('Uhrzeit', inplace=True)
		data = add_delta_value_columns(data)
		database.write(data)

	my_logger.info("========= Skript wird beendet ===========================")	

def parse_args():
	""" Parse the args from main. """
	parser = argparse.ArgumentParser(description='Read, import and export smart home self.data.')
	parser.add_argument('-i', '--import_from', type=str,
						help='name of CSV file with smart home self.data for import to database')
	parser.add_argument('-e', '--export_to', type=str,
						help='file name for CSV export from database')
	parser.add_argument('-c', '--config', type=str,
						default='Datalogger.config',
						help='configuration file')
	return parser.parse_args()

def dateparse(dates):
	if '/' in dates[0]:
		d = [pd.datetime.strptime(date, '%d/%m/%Y %H:%M') for date in dates]
	if '-' in dates[0]:
		d = [pd.datetime.strptime(date, '%Y-%m-%d %H:%M:%S') for date in dates]			
	return d

def add_delta_value_columns(data):
	previous_readings = pd.DataFrame()
	pickle_file = 'previous_readings.pickle'
	if os.path.isfile(pickle_file):
		previous_readings = pd.read_pickle(pickle_file)
	data.to_pickle(pickle_file)
	data = previous_readings.append(data)
	data = calculate_delta_values(data)
	data = data.drop(data.index[0]) # remove the row with the previous reading
	return data

class CSV_Data:
	
	def __init__(self, CSV_file_name):
		self.CSV_file_name = CSV_file_name
		self.data = pd.DataFrame()

	def import_data(self):
		self.data = pd.read_csv(self.CSV_file_name,
						   		header=0,
						   		index_col=0,
						   		parse_dates=True,
						   		date_parser=dateparse,
						   		sep=';',
						   		decimal=',',
						   		dtype = str,
						   		encoding='utf-8')
		# remove unit columns
		cols = [c for c in self.data.columns if c[:2]!=' .' and c!=' ']
		self.data = self.data[cols]
		# remove all 'Unnamed:' columns
		self.data.drop(labels=[col for col in self.data.columns if 'Unnamed:' in col],
				  	   axis=1, inplace=True)
		# map old to new column names
		self.data.columns = self.__map_old_to_new_column_names(self.data.columns)
		# set seconds to '00'
		self.data.index = self.data.index.map(lambda x: x.replace(second=0))
		# fix multiple comma bug in YouLess meter columns
		self.data['Haushaltstromzaehler - cnt'] = self.data[['Haushaltstromzaehler - cnt']].apply(self.__fix_comma_bug, axis=1)
		self.data['Waermepumpenstromzaehler - cnt'] = self.data[['Waermepumpenstromzaehler - cnt']].apply(self.__fix_comma_bug, axis=1)
		# convert decimal character ',' to '.'
		self.data = self.data.replace({',': '.'}, regex=True)
		# set 'NaN' to zero, required to convert column types
		self.data = self.data.fillna(0)
		# set column types
		self.data = set_column_types(self.data)
		# calculate delta values
		self.data = calculate_delta_values(self.data)
		# set column types
		self.data = set_column_types(self.data)
		# encode column names to 'utf-8', required before writing to database 
		self.data.columns = [col.encode('utf-8') for col in self.data.columns]
		return self.data
	
	def __map_old_to_new_column_names(self, old_column_names):
		with open('old_new_column_map.json', 'r') as f:
			old_to_new = json.load(f, encoding='utf-8')
		new_column_names = []
		for old_col in old_column_names:
			if old_col in old_to_new.keys():
				new_col = old_to_new[old_col]
			else:
				new_col = old_col
			new_column_names.append(new_col)
		return new_column_names
	
	def __fix_comma_bug(self, row):
		"""
		Excel converted YouLess meter cnt values incorrectly. The conversion
		generated float numbers with two ',' characters. E.g. '9,147,995' instead 
		of '9147,995'. This function will remove all but the last comma character.
		"""
		for col in row:
			new = col
			if not(pd.isnull(col)):
				loc = [m.start() for m in re.finditer(',', col)]
				if len(loc) > 1:
					new = col.replace(',', '', len(loc)-1)
		return new
	
def calculate_delta_values(data):
	"""
	A number of values are steadily increasing values. These values are 
	defined in section "delta_columns" in the script configuration file.
	In this function we calculate the difference to get the rate of change.
	"""
	delta_columns = [c.encode('utf-8') for c in configuration['delta_columns']]
	for c in delta_columns:
		if c in data.columns:
			delta_col_name = c + ' - delta'
			data[delta_col_name] = data[c].diff()
	data = data.fillna(value=0)
	return data

class Database:

	def __init__(self, host, port, dbuser, dbuser_password, dbname, dbmeasurement):
		self.dbmeasurement = dbmeasurement
		self.client = DataFrameClient(host, port, dbuser, dbuser_password, dbname)
		my_logger.info('Datenbankkonfiguration: host: {}:{}, Nutzer: {}, Datenbank: {}'.format(host, port, dbuser, dbname))

	def write(self, data):
		if not data.empty:
			no_lines = len(data)
			no_values = len(data.columns)
			print("Schreibe {} Zeilen mit je {} Werten in die Datenbank.".format(no_lines, no_values))
			my_logger.info("Schreibe {} Zeilen mit je {} Werten in die Datenbank.".format(no_lines, no_values))
			lines_per_chunk = 500
			lines_written = 0
			for _, data_chunk in data.groupby(np.arange(len(data))//lines_per_chunk):
				try:
					self.client.write_points(data_chunk, self.dbmeasurement,
										 	 protocol='json')
					lines_written = lines_written + len(data_chunk)
					print("{}/{} Zeilen geschrieben.".format(lines_written, no_lines))
					my_logger.info("{}/{} Zeilen geschrieben.".format(lines_written, no_lines))
				except requests.exceptions.ConnectionError as err:
					my_logger.error("Schreiben in Datenbank fehlgeschlagen. Fehlermeldung: '{}'.".format(err))
		else:
			my_logger.error("Keine Daten zum Schreiben in die Datenbank vorhanden.")
			
	def read(self):
		# to be developed
		pass

class Tecalor:
	
	def __init__(self):
		self.browser_obj = Browser()
		self.browser_obj.set_handle_robots(False)
		self.data = pd.DataFrame()

	def read_measurements(self, data_URLs):
		if 'file:' in cfg['TecalorDataURLs'][0]:
			# read data from html files, used for testing purposes
			# when offline
			for data_URL in data_URLs:
				my_logger.info("Lese Tecalor Daten von Datei '%s'. "
							   "Diese wird nur zu Testzwecken genutzt.", data_URL)
				self.data = pd.concat([self.data,
									   self.__extract(data_URL)],
									   axis=1)
		else: # get the data from the Tecalor website
			if self.__connect_to_website(cfg['TecalorLoginURL']):
				if self.__login_to_website():
					my_logger.info("Anmeldung an der Tecalor Webseite erfolgreich.")
					for data_URL in data_URLs:
						my_logger.info("Aufruf der Webseite mit den Messdaten: %s", data_URL)
						self.data = pd.concat([self.data, 
											   self.__extract(data_URL)],
											   axis=1)
			else:
				my_logger.error("Anmeldedialog der Tecalor Webseite nicht gefunden.")
		return self.data

	def __connect_to_website(self, TecalorLoginURL):
		my_logger.info("Verbinde zur Tecalor Webseite '%s'.", TecalorLoginURL)
		login_form_found = False
		try:
			self.browser_obj.open(TecalorLoginURL)
		except httplib.BadStatusLine:
			pass
		except:
			my_logger.error("Verbindung zur Tecalor Webseite fehlgeschlagen. Fehlermeldung: '%s'.", sys.exc_info())
			return login_form_found
		# Find the login form
		for form in self.browser_obj.forms():
			if form.attrs['id'] == 'werte':
				login_form_found = True
				self.browser_obj.form = form
				break
		return login_form_found
	
	def __login_to_website(self):
		my_logger.info("Tecalor Webseite gefunden. Versuche anzumelden.")
		self.browser_obj["user"] = cfg['TecalorUserName']
		self.browser_obj["pass"] = cfg['TecalorPassword']
		response = self.browser_obj.submit()
		text = response.read()
		if text.find("Login fehlgeschlagen!") > 0:
			my_logger.error("Anmeldung an der Tecalor Webseite fehlgeschlagen.")
			return False
		return True

	def __extract(self, data_URL):
		response = self.browser_obj.open(data_URL).read()
		soup = BeautifulSoup(response, "lxml")
		# A test to resonably ensure we have retrieved the web page with the
		# Tecalor status data.
		if soup.title.string == "STIEBEL ELTRON Reglersteuerung":
			my_logger.info("Webseite mit den Messdaten empfangen.")
			# Setting up a few things
			keys = []
			values = []
			units = []
			# Regular expression needs to be set to unicode in order to catch
			# the ° and ³ characters.
			temp_re = re.compile(ur" °C| %| Hz| m³/h| bar| kWh| MWh| h", flags=re.UNICODE)
			# Extract all tables that contain the data keys and values
			tables = soup.findAll("table", {"class" : "info"})
			for table in tables:
				# Data is presented in groups. Extract group name to be used later
				# as prefix for the key names.
				dataCategory = unicode(table.tr.th.string)
				# Extract the keys.
				for key in table.findAll("td", {"class" : ["key", "key round-leftbottom"]}):
					keys.append(dataCategory + " - " + unicode(key.string))
				# Extract the values.
				for value in table.findAll("td", {"class" : ["value", "value round-rightbottom"]}):
					# There are a few cases where the value is an image to show "on"
					# or "off". We just save the image file name. Not sure if it's
					# useful just saving the data while we are here.
					if value.findChildren("img"):
						values.append(value.img["src"])
						units.append("")
					else:
						# For any other value __extract the value and ...
						valueString = unicode(value.string)
						m = temp_re.search(valueString)
						if m != None:
							# ... the unit. I'm saving the unit in a seperate
							# column in the CSV file. Helps when processing the data
							# later in Excel.
							units.append(m.group(0).strip())
							valueString = re.sub(m.group(0), "", valueString)
						else:
							units.append("")
						values.append(valueString.strip())
	
			my_logger.info("{} Werte ausgelesen.".format(len(keys)))
	
			# Remove all the data elements with ".png" as values. Not needed as it
			# only represents on/off status data. These values are not always shown
			# and therefore cause the number of data elements to vary. This causes
			# problems with the CSV file.
			my_logger.info("Loesche Messdaten die nur einen Ein- oder Aus-Status darstellen.")
			toDelete = [i for i, x in enumerate(values) if (x.find(".png")>0)]
			keys = [i for j, i in enumerate(keys) if j not in toDelete]
			values = [i for j, i in enumerate(values) if j not in toDelete]
			units = [i for j, i in enumerate(units) if j not in toDelete]
		else:
			my_logger.error("Webseite mit den Messdaten nicht gefunden.")	
		# generate pandas data frame, to be ready for writing to InfluxDB
		# 'units' are not used as of now, possibly for later use
		data = pd.DataFrame(data=[values], columns=keys)
		# convert decimal character
		data = data.replace({',': '.'}, regex=True)
		# set column types
		data = set_column_types(data)
		# encode column names to 'utf-8'
		data.columns = [col.encode('utf-8') for col in data.columns]		
		return data

class YouLess:
	
	def __init__(self):
		self.data = pd.DataFrame()
		pass
	
	def read_measurements(self, data_URLs):
		keys = []
		values = []
		units = []
	
		for data_URL in data_URLs:
			my_logger.info("Verbinde zu YouLess Meter '" + data_URL["name"] + \
						   "' ueber Adresse '" + data_URL["url"] + "' abzurufen.")
			try:
				page = urllib2.urlopen(data_URL["url"])
			except urllib2.HTTPError, err:
				my_logger.info("Der folgended HTTP Fehlercode ist aufgetreten: " + err.code)
			except urllib2.URLError, err:
				my_logger.info("Der folgended Verbindungsfehler ist aufgetreten: " + err.reason[1])
			else:
				my_logger.info("Verbindung zu '" + data_URL["name"] + "' hergestellt.")
				result = json.load(page)
				for r in result:
					keys.append(data_URL["name"] + " - " + r)
					if str(result[r]) == "&bull;":
						# replace with '*', the ';' in the unicode string causes
						# an additional unintended column
						values.append(unicode("*"))
					else:
						values.append(unicode(str(result[r])))
					units.append("")
				my_logger.info('{} Werte ausgelesen.'.format(len(result)))
		# generate pandas data frame, to be ready for writing to InfluxDB
		# 'units' are not used as of now, possibly for later use
		self.data = pd.DataFrame(data=[values], columns=keys)
		# convert decimal character
		self.data = self.data.replace({',': '.'}, regex=True)
		# set column types
		self.data = set_column_types(self.data)
		# encode column names to 'utf-8'
		self.data.columns = [col.encode('utf-8') for col in self.data.columns]		
		return self.data

def set_column_types(df):
	column_types = configuration['column_types']
	for col in df.columns:
		df[col] = df[col].astype(column_types[col])
	return df

if __name__ == "__main__":
	# get script configuration
	args = parse_args()
	with open(args.config, 'r') as f:
		configuration = json.load(f, encoding='utf-8')
	cfg = configuration['configuration']
	# set up logging, rotating log file, max. file size 100 MBytes
	my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)
	formatter = logging.Formatter('%(asctime)s - %(message)s')
	handler = logging.handlers.RotatingFileHandler(cfg['LogFile'],
												   maxBytes=104857600,
												   backupCount=1)
	handler.setFormatter(formatter)
	my_logger.addHandler(handler)
	# off we go!
	main()
