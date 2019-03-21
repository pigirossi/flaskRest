import argparse as ap
from pprint import pprint
from mongo_libs import MongoManager
from data_translator import DataHandler


def printAllMusicalWorks(mongoManager):

    for musical_work in mongoManager.retrieve_iswc_works():
        pprint(musical_work)


if __name__ == '__main__':

    parser = ap.ArgumentParser()

    parser.add_argument('-mongohost', action="store", dest="host", default="localhost")
    parser.add_argument('-mongoport', action="store", dest="port", default="27017")
    parser.add_argument('-mongouser', action="store", dest="user", default=None)
    parser.add_argument('-mongopwd', action="store", dest="pwd", default=None)
    parser.add_argument('-datafile', action="store", dest="filecsv", default="works_metadata.csv")

    args = parser.parse_args()

    if args.pwd is not None and args.user is not None:
        mongoUri = args.user + ":" + args.pwd + "@mongodb://" + args.host + ":" + args.port
    else:
        mongoUri = "mongodb://" + args.host + ":" + args.port

    data_handler = DataHandler(csv_fname=args.filecsv)
    mongoManager = MongoManager(mongoUri)

    for musical_work_mongo in data_handler.get_musical_works_list():
        mongoManager.insert_musical_work(musical_work_mongo)

    printAllMusicalWorks(mongoManager)
