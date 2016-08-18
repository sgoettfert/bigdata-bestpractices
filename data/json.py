#!/usr/bin/python
import csv

# file names
source_file = "ratings.csv"
target_file = "data.json"

# csv configs
default_delimiter = ","
default_quote = "\""

# transform ratings from TSV to JSON
def build_json(f_source, f_target):
    with open(f_source, newline='') as csv_source:
        reader = csv.reader(csv_source, delimiter=default_delimiter, quotechar=default_quote)

        with open(f_target, 'w') as writer:
            for line in reader:
                json = '{"userId":' + line[0] + ',"movieId":' + line[1] + \
                ',"rating":' + line[2] + ',"timestamp":' + line[3] + '}\n'
                writer.write(json)

if __name__ == "__main__":
    build_json(source_file, target_file)

