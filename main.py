"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""

from src.some_storage_library import SomeStorageLibrary as SSL


if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')

    cols_file = open('data/source/SOURCECOLUMNS.txt', 'r')
    cols = cols_file.read().split('\n')
    cols_file.close()

    data_file = open('data/source/SOURCEDATA.txt', 'r')
    data = data_file.read().replace('|', ',')
    data_file.close()

    cols_sorted = sorted(cols, key = lambda x: int(x.split('|')[0]))

    ls_name = [item.split('|')[1] for item in cols_sorted]

    final_table = ','.join(ls_name) +'\n' + data
    f = open('column_data.csv', 'w')
    f.write(final_table)
    f.close()
    
    SSL().load_csv('column_data.csv')

