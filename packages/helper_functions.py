import requests
import os
from tqdm import tqdm
import yaml


# Saves a csv-like txt file to a csv.
def save_to_csv(file_path, desired_csv_file_name):
    with open(file_path, 'r') as text_file:
        pass


# Splits a list into multiple lists of size n, where n <=l.
# If n doesn't divide evenly into l, then the last chunk will be less than n.
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


# Downloads a file if given a url and file_name and stores it in the local PC.
def download_file(url, file_name, default_folder='input_data'):
    # Get the response from URL.
    with requests.get(url, stream=True) as r:
        chunkSize = 1024  # Download 1024 bytes at a time.
        with open(get_path_dir(default_folder, file_name), 'wb') as raw_file:
            for chunk in tqdm(iterable=r.iter_content(chunk_size=chunkSize), total=int(r.headers['Content-Length']) / chunkSize, unit='KB', desc="Downloading %s" % file_name):
                raw_file.write(chunk)


"""
Purpose: The get_path_dir is responsible for returning a string of a valid file path to a file in the AgAuto cwd if
given a valid directory within the AgAuto cwd and a file within the cwd.

Parameters:
    - directory: Must be a folder that exists within the AgAuto cwd.
    - file_name: The file that you want to access within directory.
    - create: If True, then get_path_dir will not care that the file doesn't exist in directory yet as it assumes
    it will be created using the file path that get_path_dir returns.
"""


def get_path_dir(directory, file_name, create=True):
    # Gets the path of the working directory (i.e. AgAuto's working directory).
    cwd = os.getcwd()
    # Add directory to the working directory path.
    file_base_dir = os.path.join(cwd, directory)
    # Add file_name to the new path created above.
    file_path = os.path.join(file_base_dir, file_name)

    # If the directory doesn't exist then raise an Exception.
    if not os.path.exists(file_base_dir):
        raise Exception('Directory %s does not exist within working directory.' % directory)
    # Raise an exception only if the user specifies create = False. Otherwise, assume they will create after.
    if not create:
        if not os.path.exists(file_path):
            raise Exception('File %s does not exist within %s.' % (file_name, directory))

    return file_path


"""
Purpose: cleanData's purpose is to open filename (which could be "mawp24raw.txt) and remove all instances of
"-7999", "-99", "NAN", and the last line of the textfile.

Parameters:
    filename - this parameter is the file name of the text file to be cleaned.
"""


def cleanData(filename):
    # Change this later to a more general case. Maybe user input?
    try:
        download_file('https://mbagweather.ca/partners/mbag' + '/' + filename,  filename, "")
        file_wip = open(filename, "r")
        new_contents = ""

        count = 0

        for line in file_wip:
            count += 1

        file_wip.seek(0)

        for line in tqdm(file_wip, desc="Cleaning %s" % filename, total=count):

            # If the length of the line is less than or equal to 1, then don't add it to the output.
            if len(line) > 1:
                append_line = line.replace("-7999", "").replace("-99", "").replace("NAN", "").replace("7999", "")
                new_contents = new_contents + append_line

        file_wip.close()
        file_wip = open(filename, 'w')
        file_wip.write(new_contents)
        file_wip.close()

    except IOError as io:
        print(io)
        print("mawp24raw.txt or mawp60raw.txt were not found. Please check directory.")


def get_sql_column_names():
    with open(get_path_dir('config_files', 'mawp_60_column_config.yaml'), 'r') as yaml_config:
        yaml_load = yaml.safe_load(yaml_config)
        for each_column in yaml_load:
            yield each_column
