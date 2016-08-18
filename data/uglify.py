#!/usr/bin/python
import csv
import os
import random
import string

# file names
source_file_data = "ratings.csv"
source_file_item = "movies.csv"
target_dir = "uglified"

# csv configs
default_quote = "\""
source_delimiter = ","

# percentage of whole data set to be uglified
weight_uglify = 40

# uglify actions with their frequency weight
actions = {
    "to_null": 1,
    "add_character": 2,
    "add_number": 2,
    "add_symbol": 1
}

actions_weighted = []


def build_weights():
    for action in actions:
        for i in range(0, actions.get(action)):
            actions_weighted.append(action)


def get_uglify_method():
    return random.choice(actions_weighted)

"""
uglifying methods
"""


def ugl_to_null(field):
    return ""


def ugl_add_number(field):
    return str(field) + str(random.randint(0, 10))


def ugl_add_character(field):
    return str(field) + random.choice(string.ascii_letters)


def ugl_add_symbol(field):
    symbols = ['.', '-', '#', '*']
    return str(field) + random.choice(symbols)


def uglify_line(line):
    action = get_uglify_method()
    pos = random.randint(0, len(line) - 1)
    field = line[pos]
    result = list(line)

    if action == "to_null":
        result[pos] = ugl_to_null(field)

    elif action == "add_number":
        result[pos] = ugl_add_number(field)

    elif action == "add_character":
        result[pos] = ugl_add_character(field)

    elif action == "add_symbol":
        result[pos] = ugl_add_symbol(field)

    return result


def decide_action():
    chance = random.randint(0,100)

    if chance < weight_uglify:
        return True
    else:
        return False


def uglify_file(file_name, target_delimiter):
    with open(file_name, newline='') as csv_source:
        reader = csv.reader(csv_source, delimiter=source_delimiter, quotechar=default_quote)

        with open(target_dir + "/" + file_name, 'w') as csv_target:
            writer = csv.writer(csv_target, delimiter=target_delimiter, quotechar=default_quote)

            for line in reader:

                if decide_action():
                    ugly = uglify_line(line)
                    writer.writerow(ugly)
                else:
                    writer.writerow(line)


if __name__ == "__main__":

    # create target directory
    os.mkdir(target_dir)

    # build weights according to settings
    build_weights()

    # start uglyfying the records
    uglify_file(source_file_data, "\t")
    uglify_file(source_file_item, ",")

