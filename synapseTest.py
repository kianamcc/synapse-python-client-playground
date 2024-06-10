import synapseclient
from synapseclient import Project, Folder, File, EntityViewSchema, Schema, Column, Table, Row, RowSet, as_table_columns, build_table, MaterializedViewSchema
import synapseutils
import pandas as pd
import os

import tempfile

# login
syn = synapseclient.Synapse()
syn.login()

# get a synapse entity
entity = syn.get('syn59873915')

# open on the web
# syn.onweb('syn59873915')

# create a project
project = Project('Kiana_Python_Client_Playground')
project = syn.store(project)

print(f"Project ID: {project.id}")
print(f"I created my projectt on: {project.createdOn}")
print(f"ID of user who created this project: {project.createdBy}")
print(f"My project was last modied on: {project.modifiedOn}")
print(f"Project name: {project.name}")

# create a folder
# data_folder = Folder(name="Data", parent=project)
# data_folder = syn.store(data_folder)

# create subfolders
# folder_notes_2020 = Folder(name="Notes_2020", parent=data_folder)
# folder_notes_2020 = syn.store(folder_notes_2020)

# folder_notes_2021 = Folder(name="Notes_2021", parent=data_folder)
# folder_notes_2021 = syn.store(folder_notes_2021)

# list all folders and files
for directory_path, directory_names, file_name in synapseutils.walk(
    syn=syn, synId=project.id, includeTypes=["file"]
):
    for directory_name in directory_names:
        print(f"Directory ({directory_name[1]}): {directory_path[0]}/{directory_name[0]}")
    for file in file_name:
        print(f"File ({file[1]}): {directory_path[0]}/{file[0]}")


# create files - need to use tempfile
# temp = tempfile.NamedTemporaryFile(prefix='coffee', suffix='.txt')

# with open(temp.name, "w") as temp_f:
#     temp_f.write("I like iced lattes!")
# filepath = temp.name
# test_entity = File(filepath, name="coffee.txt", description='Fancy new data', parent=data_folder)
# test_entity = syn.store(test_entity)

# notes_2020 = File(path=os.path.expanduser("/Users/kmccullough/Documents/synapse-playground/notes_2020.txt"), parent=folder_notes_2020)
# notes_2020_file = syn.store(notes_2020)

# notes_2021 = File(path=os.path.expanduser("/Users/kmccullough/Documents/synapse-playground/notes_2021.txt"), parent=folder_notes_2021)
# notes_2021_file = syn.store(notes_2021)

# annotating
# annotate_entity = syn.get(test_entity.id)
# annotate_entity['coffee_syrup'] = "brown sugar"
# syn.store(annotate_entity)

# annotation_values = {
#     "species": "Homo sapiens",
#     "dataType": "geneExpression"
# }

# existing_annotations = syn.get_annotations(notes_2020_file)
# existing_annotations.update(annotation_values) # merge new annotations with anything exisiting
# existing_annotations = syn.set_annotations(annotations=existing_annotations)

# versioning
# temp = tempfile.NamedTemporaryFile(prefix='second', suffix='.txt')
# with open(temp.name, "w") as temp_f:
#     temp_f.write("First line of text \n")

# version_entity = File(temp.name, name="first", parent=data_folder)
# version_entity = syn.store(version_entity)
# print('initial: ', version_entity.versionNumber)

# with open(temp.name, "a") as temp_f:
#     temp_f.write("Second line of text")
# version_entity = File(temp.name, name="second", parent=data_folder)
# version_entity = syn.store(version_entity)
# print('modification: ', version_entity.versionNumber)

# Provenance - effects file versioning
# version_1 = syn.get(version_entity, version=1)

# create an activity implicitly
# provenance_entity = syn.store(version_entity, used=[version_1.id]) # not common to store provenance
# print('provenance entity', provenance_entity.versionNumber)

# create an activity explicitly
# activity = Activity(name="Updated data", description="Added another line of text", used=[version_1.id])

# delete files
file_to_delete = 'syn59880803'
syn.delete(file_to_delete)

# file views
file_view = EntityViewSchema(name="myTable", parent=project.id, scopes=[project.id])
file_view_entity = syn.store(file_view)

# query files
query = syn.tableQuery(f"select * from {file_view_entity.id}")
query_results_df = pd.DataFrame(query)
# print(query_results_df)

# tables
# build table sets the schema (columns of table)
# table = build_table('Cats', project, '/Users/kmccullough/Documents/synapse-playground/cats.csv')
# syn.store(table)

# results = syn.tableQuery("select * from %s where Age > 1" % table)
# for row in results:
#     print(row)

# custom schema
# cols = [
#     Column(name="Name", columnType="STRING", maximumSize=25),
#     Column(name="Age", columnType="INTEGER", maximumSize=25),
#     Column(name="Breed", columnType="STRING", maximumSize=25),
#     Column(name="Color", columnType="STRING", maximumSize=25),
# ]

# schema = Schema(name="Custom Schema", columns=cols, parent=project)
# stored_schema = syn.store(schema)
# table_2 = Table(schema, "/Users/kmccullough/Documents/synapse-playground/cats_custom.csv")
# table_2 = syn.store(table_2)
# print("table_2 schema id: ", stored_schema.id)

# results = syn.tableQuery(f"select * from {table_2.schema.id} where Age > 1")
# for row in results:
#     print(row)

# table from dataframe
df = pd.read_csv("/Users/kmccullough/Documents/synapse-playground/cats_dataframe.csv", index_col=False)
# table_3 = build_table("Dataframe Table", project, df)
# table_3 = syn.store(table_3)

# dataframe w/ custom schema
dataframe_schema = Schema("Dataframe Custom Schema Table", columns=as_table_columns(df), parent=project)
stored_dataframe_schema = syn.store(dataframe_schema)
# table_3 = syn.store(Table(dataframe_schema, df))

# results = syn.tableQuery(f"SELECT * FROM {stored_dataframe_schema.id} WHERE Color='Brown'")
# df = results.asDataFrame()
# print("Brown cats: ", df)

# add rows from another file
# table_3 = syn.store(Table(table_3.schema.id, "/Users/kmccullough/Documents/synapse-playground/more_cats.csv"))

# add rows from a list of data
# new_rows = [["Bop", 5, "Tabby", "Grey"]]
# table_3 = syn.store(Table(dataframe_schema, new_rows))

# update - requires etag
# results = syn.tableQuery(f"SELECT * FROM {stored_dataframe_schema.id} WHERE Color='Brown' AND Breed='Siamese' AND Age > 4 AND Name Like 'Sim%'")
# df = results.asDataFrame()
# print(len(df))
# df['Name'] = ["Natty", "Zozo", "Lola", "Cheese", "Poisson", "Peppy", "Chip", "Pep", "Tom", "Jimm", "Domo", "Donny", "Dott", "Pop", "Popo"]
# table = syn.store(Table(dataframe_schema, df, etag=results.etag))

# changing table structure
# adding columns
# schema = syn.get("syn60120806")
# weight_column = syn.store(Column(name="Weight", columnType="INTEGER"))
# schema.addColumn(weight_column)
# schema = syn.store(schema)

# table = syn.get("syn60120806")

# cols = [
#     Column(name='Name', columnType='STRING', maximumSize=50),
#     Column(name='Picture', columnType='FILEHANDLEID')]
# schema = syn.store(Schema(name='Cat Images', columns=cols, parent=project))

# table data
# data = [["John", '/Users/kmccullough/Documents/synapse-playground/notes_2021.txt'],
#         ["Kenny", '/Users/kmccullough/Documents/synapse-playground/notes_2021.txt']]

# upload file(s)
# for row in data:
#     file_handle = syn.uploadFileHandle(row[1], parent=project)
#     row[1] = file_handle['id']

# store the table data
# row_reference_set = syn.store(RowSet(schema=schema, rows=[Row(r) for r in data]))

# query the table and download our album covers
# results = syn.tableQuery(f"SELECT Name, Picture FROM {schema.id} WHERE Name = 'John'")
# test_files = syn.downloadTableColumns(results, ['Image'])

# rename/modify column - must remove and add new column
# cols = syn.getTableColumns(schema)
# for col in cols:
#     if col.name == "Weight":
#         schema.removeColumn(col)
# weight_column_2 = syn.store(Column(name="Weight2", columnType="INTEGER"))
# schema.addColumn(weight_column_2)
# schema = syn.store(schema)

# table attached files

# deleting rows

# deleting the whole table
# syn.delete(schema)

# materialized view
table = syn.get("syn60120806")
sql = "SELECT * FROM syn60120806 T1 JOIN syn60120805 T2 on (T1.Name = T2.Name)"
schema = syn.store(MaterializedViewSchema(name="Materialized Table", parent=project, definingSQL=sql))