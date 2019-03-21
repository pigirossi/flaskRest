import csv
import os

from flask import Flask, request, make_response
from flask_restful import Resource, Api

from mongo_libs import MongoManager
from data_translator import DataHandler


app = Flask(__name__)
api = Api(app)

mongoUri = os.environ.get('MONGO_URI', 'mongodb://localhost:27017')


class FindMusicalWork(Resource):

    def __init__(self):
        self.mongoManager = MongoManager(mongoUri)

    def get(self, iswc):
        work = self.mongoManager.find_work_by_iswc(iswc)

        return work if work is not None else {'message': 'item not found'}


class ExportData(Resource):

    def __init__(self):
        self.data_handler = DataHandler()
        self.mongoManager = MongoManager(mongoUri)

    def _csv2string(self, data):
        res = [",".join(item) for item in data]
        return "\n".join(res)


    def get(self):

        all_works = self.mongoManager.retrieve_iswc_works()

        if len(all_works) <= 0:
            return ({"message" : "database has no items"})

        list_of_works = self.data_handler.musical_works_to_list(all_works)

        #translate worklist to csv format
        response = make_response(self._csv2string(list_of_works))
        response.headers['Content-Disposition'] = "inline"
        response.headers["Content-type"] = "text/csv"

        return response


class ImportData(Resource):

    def __init__(self):
        self.data_handler = DataHandler()
        self.mongoManager = MongoManager(mongoUri)


    def post(self):

        file = request.files['file']

        # store the file contents as a string
        fstring = file.read()

        for row in csv.DictReader(fstring.decode("utf-8").splitlines()):
            musical_work = self.data_handler.create_musical_work(row)
            insert_result = self.mongoManager.insert_musical_work(musical_work)
            if bool(insert_result) is False:
                return {"message": "error inserting musical work with iswc " + musical_work['iswc']}, 400

        return {'message':  "OK"}


api.add_resource(FindMusicalWork, "/findwork/<string:iswc>")
api.add_resource(ExportData, "/exportdata")
api.add_resource(ImportData, "/importdata")


if __name__ == '__main__':
    app.run(debug=True)
