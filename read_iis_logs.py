import pandas as pd
import os
from multiprocessing import Pool, Value
from datetime import datetime
import time

all_dfs = []
finished = 0
counter = None

def init(args):
    ''' store the counter for later use '''
    global counter
    counter = args

def convert_to_df(log_file, total_files):
    global counter
    print(f'{datetime.now()}: converting {log_file}...')
    my_df = pd.DataFrame()
    my_cols = {}
    with open(log_file) as f:
        lines = f.readlines()
        for read_line in lines:
            read_line = read_line.replace('\n', '')
            line = read_line.split(' ')
            if line[0] == '#Fields:':
                columns = line[1:len(line)]
                for c in columns:
                    index = columns.index(c)
                    my_cols[index] = c

            elif any(my_cols) and (len(line) == len(columns)):
                row = {}
                for col_index, col_name in my_cols.items():
                    row[col_name] = line[col_index]
                my_df = my_df.append(row, ignore_index=True)

    with counter.get_lock():
        counter.value += 1
    print(f'finished conversion ({counter.value}/{total_files}): {log_file}')
    return my_df

def apply_async_with_callback():
    counter = Value('i', 0)
    dir_name = "FULL PATH TO LOCAL DIRECTORY CONTAINING .log FILES"
    files = os.listdir(dir_name)
    files = [os.path.join(dir_name, f) for f in files]
    total_files = len(files)
    all_dfs = []

    pool = Pool(initializer = init, initargs = (counter, ), processes=8)
    calls = [pool.apply_async(convert_to_df, args=(f,total_files)) for f in files]
    for call in calls:
        result = call.get()
        if result is not None:
            all_dfs.append(result)

    pool.close()
    pool.join()

    if len(all_dfs) > 0:
        total_df = pd.concat(all_dfs)
        total_df['datetime'] = total_df.apply(lambda row: f'{row.date} {row.time}', axis=1)
        csv_file = f'iis_logs_{time.time()}.csv'
        total_df.to_csv(csv_file, index=False)
        print(f'saved data to {csv_file}')
        return csv_file

def sort_dataframe(csv_file):
    df = pd.read_csv(csv_file)
    df = df.sort_values(by=['date', 'time'], ascending=True)
    sorted_file = csv_file.replace('iis_logs', 'iis_logs_sorted')
    df.to_csv(sorted_file, index=False)
    print(f'saved sorted data to {sorted_file}')

if __name__ == "__main__":
    csv_file = apply_async_with_callback()
    sort_dataframe(csv_file)




