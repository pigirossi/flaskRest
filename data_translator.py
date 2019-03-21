import csv

class DataHandler:

    def __init__(self, csv_fname=None):
        self.csv_fname = csv_fname

    def _get_works_metatada_dict(self):
        with open(self.csv_fname, "r") as works_metadata:
            for record in csv.DictReader(works_metadata):
                yield record

    def create_musical_work(self, item):
        musical_work = {
            'title': item['title'],
            'contributors': [contributor.strip() for contributor in item['contributors'].split("|")],
            'iswc': item['iswc'],
            'sources': [item['source']],
            'sources_id': {item['source']: item['id']}
        }

        return musical_work

    #return all musical work as a list of string
    def get_musical_works_list(self):
        return [self.create_musical_work(musicalWork) for musicalWork in self._get_works_metatada_dict()]


    def musical_works_to_list(self, musical_works):

        result = list()

        keys = list(musical_works[0].keys())

        result.append(keys)

        for musical_work in musical_works:
            line = list()
            for key in keys:
                if isinstance(musical_work[key], list):
                    line.append("|".join(musical_work[key]))

                elif isinstance(musical_work[key], dict):
                    values = [c for c in musical_work[key].values()]
                    line.append("|".join(values))

                else:
                    line.append(musical_work[key])

            result.append(line)

        return result







