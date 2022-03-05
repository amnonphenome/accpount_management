import csv
class City:
    def __init__(self, row, header, the_id):
        self.__dict__ = dict(zip(header, row))
        self.the_id = the_id
    def __repr__(self):
        return self.the_id

data = list(csv.reader(open('file.csv')))
instances = [City(a, data[0], "city_{}".format(i+1)) for i, a in enumerate(data[1:])]


