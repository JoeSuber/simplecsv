__author__ = 'suber1'
from pymongo import MongoClient
import cPickle
import os
import os.path


def csvp(startingPath, csv_ext='.csv'):
    """ gathering of the input files, keeping full path info """
    print 'walking up path=', startingPath
    csvfn = [os.path.join(root, filename)
                for root, dirnames, filenames in os.walk(startingPath)
                for filename in filenames if filename.endswith(csv_ext)]
    print 'list is ', len(csvfn), ' images long'
    print 'starting with', csvfn[0]
    print 'ending with', csvfn[-1]
    return csvfn


def selections(dd=None, prompt='Choose from above'):
    """ given dict of numbered options like {1: 'choose me', ...}
    present user with options and return the chosen integer key and value string
    """
    choice = 1
    if not dd:
        print('selections: empty choice dictionary, returning 0')
        return 0, None
    for choice, dbnm in dd.viewitems():
        print('{:4}- {}'.format(choice + 1, dbnm))
    q = -1000
    while (q - 1) not in dd.keys():
        if len(dd) > 1:
            q = int(unicode(raw_input(prompt)))
        else:
            print('only one choice.. ')
            q = 1
    return q - 1, dd[q - 1]


def de_string(i, isint=True):
    """ if getting a string, return pennies on the dollar, or stripped-down string"""
    if isinstance(i, (str, unicode)):
        try:
            if isint:
                return int(i.replace(".", "").replace("$", "").strip())
            else:
                return float(i.replace("$", "").strip())
        except ValueError:
            return i.strip()
    return i


class MongizeCsv(MongoClient):
    def __init__(self, csv_filename=None, column_headers=None, db=None, splitter=u","):
        super(MongizeCsv, self).__init__()
        used_csv_fn = os.path.join(os.getcwd(), "used_csv_fn.pkl")
        used_csvs = []
        self.column_headers = column_headers
        if os.path.isfile(used_csv_fn):
            with open(used_csv_fn, 'rb') as ufob:
                used_csvs = cPickle.load(ufob)
        if not csv_filename:
            csv_fns = csvp(os.path.join(os.path.expanduser("~"), "Desktop"))
            csv_fns = [fn for fn in csv_fns if fn not in used_csvs]
            choices = {x: fn for x, fn in zip(xrange(len(csv_fns)), csv_fns)}
            a, csv_filename = selections(choices, prompt='Select from un-imported CSV files: ')
        try:
            with open(csv_filename, 'rb') as csvfob:
                self.csv_text = csvfob.read()
        except IOError as e:
            print("Error opening: {} ".format(csv_filename))
            print("Error message: {}".format(e))
        counter = 0
        if len(self.csv_text) and (not self.column_headers):
            self.csv_text = self.csv_text.splitlines()
            for counter, first_line in enumerate(self.csv_text):
                if (len(first_line) > 3) and (splitter in first_line):
                    self.column_headers = first_line.split(splitter)
                    break
                else:
                    self.column_headers = None
                    print("No column headers to be found in: {}".format(csv_filename))
        line_items, listed = {}, []
        if len(self.csv_text):
            for line in self.csv_text[counter+1:]:
                cells = line.split(splitter)
                line_items = {column.strip(): de_string(a_cell)
                              for column, a_cell in zip(self.column_headers, cells)}
                listed.append(line_items)
                print("line_items = {}".format(line_items))
        # now put listed into mongo


if __name__ == "__main__":
    mine = MongizeCsv()
    print(mine.database_names())
