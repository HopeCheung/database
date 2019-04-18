# Your implementation goes in this file.
from BaseDataTable import BaseDataTable
import sys, os, csv
from tabulate import tabulate

rel_path = os.path.dirname(os.path.realpath('__file__'))

class CSVDataTable(BaseDataTable):
	csvdir = rel_path + "/../Data/"
	def __init__(self, table_name, key_columns=None):
		self.table_name = table_name
		self.key_columns = key_columns
		self.table = []
		self.table_file = CSVDataTable.csvdir + table_name + ".csv"

	def __str__(self):
		if not self.table:
			return "CSVTable is Empty!!!"
		else:
			if len(self.table) > 30:
				table_less = self.table[:10]
				t = tabulate(table_less)
				return t
			else:
				t = tabulate(self.table)
				return t
				
	def load(self):
		with open(self.table_file, newline="") as file:
			reader = csv.reader(file)
			for row in reader:
				self.table.append(row)
		
	def find_by_primary_key(self, key_fields, field_list=None):
		ans_row = None
		if not isinstance(key_fields, list):
			raise ValueError("Wrong key_list!")
		for row in self.table[1:]:
			if all([key in row for key in key_fields]):
				ans_row = row
		if ans_row == None:
			return 
		dic = {}

		if field_list == None:
			orders = list(range(len(self.table[0])))
		else:
			orders = [self.table[0].index(field) for field in field_list]
		for i in orders:
			dic[self.table[0][i]] = ans_row[i]
		return dic

	def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
		from DerivedDataTable import DerivedDataTable
		if not isinstance(template, dict):
			 raise ValueError("Wrong template!")
		new_table = []
		for row in self.table[1:]:
			if all([template[t] == row[self.table[0].index(t)] for t in template]):
				new_table.append(row)
		ans = []
		title = self.table[0] if field_list == None else field_list
		for row in new_table:
			ans.append({t:row[self.table[0].index(t)] for t in title})
		new_derivedTable = DerivedDataTable("Template Table", ans)
		return new_derivedTable

	def insert(self, new_record):
		if isinstance(new_record, dict) and all(x in self.table[0] for x in new_record) and all(x in new_record for x in self.key_columns):
			new_table = [None] * len(self.table[0])
			for row in self.table[1:]:
				if all(new_record[x] == row[self.table[0].index(x)] for x in self.key_columns):
					raise ValueError("Wrong key!")
			for elem in self.table[0]:
				new_table[self.table[0].index(elem)] = new_record[elem] if elem in new_record else None
			self.table.append(new_table)
		else:
			raise ValueError("Wrong key!")

	def delete_by_template(self, template):
		cnt = 0
		if not isinstance(template, dict):
			raise ValueError("Wrong template!")
		for row in self.table[1:]:
			if all([template[t] == row[self.table[0].index(t)] for t in template]):
				self.table.remove(row)
				cnt = cnt + 1
		return cnt

	def delete_by_key(self, key_fields):
		cnt = 0
		if not isinstance(key_fields, list):
			raise ValueError("Wrong keys!")
		for row in self.table[1:]:
			if all([key in row for key in key_fields]):
				self.table.remove(row)
				cnt = cnt + 1
		return cnt

	def update_by_template(self, template, new_values):
		cnt = 0
		if not isinstance(template, dict):
			raise ValueError("Wrong template!")
		if isinstance(new_values, dict) and all(x in self.table[0] for x in new_values):
			for i in range(1, len(self.table)):
				row = self.table[i][:]
				if all([template[t] == row[self.table[0].index(t)] for t in template]):
					for elem in new_values:
						self.table[i][self.table[0].index(elem)] = new_values[elem]
					cnt = cnt + 1
					check = [self.table[i][self.table[0].index(elem)] for elem in self.key_columns]
					for j in range(1, len(self.table)):
						if i != j and all([key in self.table[j] for key in check]):
							self.table[i] = row
							raise ValueError("Wrong Update Values!")
			return cnt
		else:
			raise ValueError("Wrong template!")

	def update_by_key(self, key_fields, new_values):
		cnt = 0
		if not isinstance(key_fields, list):
			raise ValueError("Wrong keys!")
		if isinstance(new_values, dict) and all(x in self.table[0] for x in new_values):
			for i in range(1, len(self.table)):
				row = self.table[i][:]
				if all([key in row for key in key_fields]):
					for elem in new_values:
						self.table[i][self.table[0].index(elem)] = new_values[elem]
					cnt = cnt + 1
					check = [self.table[i][self.table[0].index(elem)] for elem in self.key_columns]
					for j in range(1, len(self.table)):
						if i != j and all([key in self.table[j] for key in check]):
							self.table[i] = row
							raise ValueError("Wrong Update Values!")
			return cnt
		else:
			raise ValueError("Wrong key!")




