import csv

def write_to_csv(file_name, line_to_write):
    with open(file_name, 'a', newline='') as f:
        f.write(line_to_write)
        f.write('\n')


def main():
    with open('players.csv', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            new_row = ",".join(row).replace(', ', ',')
            write_to_csv('players_new.csv', new_row)

main()