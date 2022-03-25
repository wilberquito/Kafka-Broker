import pyodbc

# author: wilberquito at Nexus Geographics

print('Please make sure, your needed data source driver appears in the list...')
print('Otherwise, packages like pyodbc & SQLAlchemy won\'t work as spected\n')

print('List:\n')
for driver in pyodbc.drivers():
    print(driver)
