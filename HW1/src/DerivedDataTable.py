# Your implementation goes in this file
class DerivedDataTable():
	def __init__(self, table_name, table_content):
		self.table_name = table_name
		self.table_content = table_content

	def find_by_template(self, template):
		new_table = []
		for row in self.table_content:
			if all(template[t] == row[t] for t in template):
				new_table.append(row)
		new_derivedTable = DerivedDataTable("Template Table", new_table)
		return new_derivedTable
