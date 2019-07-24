import time
from src.DBMS import DBMS as dbms
from src.FileReader import FileReader as fr
import multiprocessing as mp
import multiprocessing.pool as pool
import src.utils as utils


def main():

    start_time = time.time()

    file_reader = fr()
    config_data = file_reader.config_data

    # Create processes equal to the number of cpu's - 1
    if mp.cpu_count() >= 2:
        process_count = mp.cpu_count() - 1

    else:
        process_count = 1
    print("The number of threads is: ", process_count)

    init_time = time.time()
    print("Pre file reading duration = ", init_time - start_time)

    files = file_reader.get_files()
    get_file_time = time.time()
    print("File getting duration = ", get_file_time - init_time)

    processes = pool.Pool(process_count)
    json_dicts = processes.map(file_reader.read_JSON, files)
    processes.close()

    documents = file_reader.prepare_documents(json_dicts)

    read_time = time.time()
    print("File reading duration is: ", read_time - get_file_time)

    # Connect to db
    collection_name = utils.get_info(config_data, "COLLECTION_NAME")
    db = dbms(collection_name)

    if db.current_collection.count() != 0:
        "Database is already full"
    else:
        "Database is empty, inserting documents"
        db.insert_documents(documents)

    injection_time = time.time()
    print("Database injection time is: ", injection_time - read_time)

    db.close()

    return


if __name__ == '__main__':

    main()
