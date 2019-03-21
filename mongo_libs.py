from pymongo import MongoClient
import re


class MongoManager:
    def __init__(self, uri):

        self.client = MongoClient(uri)
        self.db = self.client.musicalWorks

    def _reconcile_works(self, musical_work_1, musical_work_2):

        reconciled_contrubutors = self._reconcile_contributors(
            [contributor for contributor in musical_work_1['contributors']],
            [contributor for contributor in musical_work_2['contributors']])

        reconcilied_soures = self._reconcile_sources(musical_work_1['sources'], musical_work_2['sources'])

        reconcilied_soures_id = self._reconcile_sources_id(musical_work_1['sources_id'], musical_work_2['sources_id'])

        reconcileWork = {
            'title': musical_work_1['title'],
            'contributors': reconciled_contrubutors,
            'iswc': musical_work_1['iswc'] if musical_work_1['iswc'] not in (None, '') else musical_work_2['iswc'],
            'sources': reconcilied_soures,
            'sources_id': reconcilied_soures_id
        }

        return reconcileWork

    def _reconcile_contributors(self, contributors1, contributors2):

        contributors1.extend(contributors2)
        self.allContributors = contributors1[:]

        reconciled_contrubutors = list(filter(self._finduplicates, self.allContributors))

        return list(set(reconciled_contrubutors))

    def _reconcile_sources(self, sources1, sources2):

        sources1.extend(sources2)

        return list(set(sources1))

    def _reconcile_sources_id(self, id1, id2):

        toRet = id1.copy()
        toRet.update(id2)

        return toRet

    def _finduplicates(self, name):

        splitted_name = re.split(r'\s+', name.lower())

        for item in [re.split(r'\s+', contributor.lower()) for contributor in self.allContributors]:
            if (set(splitted_name) < set(item)):
                return False

        return True

    def _find_by_sourceId(self, musicalWork):

        source = musicalWork['sources'][0]
        sourceId = musicalWork['sources_id'].get(source)

        queryResult = self.db.worksCollection.find_one({'sources_id.' + source: sourceId})

        return queryResult if queryResult is not None else None


    def insert_musical_work(self, musical_work):

        #if musical work DOESN'Y have iswc
        if musical_work['iswc'] in ("", None):

            #search for id medatata provider
            work_by_sourceId = self._find_by_sourceId(musical_work)

            if work_by_sourceId is not None:
                # reconcile musical works with same medatata_provider_id
                reconcilied_work = self._reconcile_works(work_by_sourceId, musical_work)
                return self.db.worksCollection.replace_one({'_id': work_by_sourceId.get('_id')}, reconcilied_work)

            return self.db.worksCollection.insert_one(musical_work)

        #if musical work HAS have iswc
        queryResult = self.db.worksCollection.find_one({'iswc': musical_work['iswc']})

        if queryResult is None:

            #search for medatata_provider_id if there is no musical work with the same iswc
            work_by_sourceId1 = self._find_by_sourceId(musical_work)

            if work_by_sourceId1 is not None:

                # reconcile musical works with same medatata_provider_id
                reconcilied_work1 = self._reconcile_works(work_by_sourceId1, musical_work)
                return self.db.worksCollection.replace_one({'_id': work_by_sourceId1.get('_id')}, reconcilied_work1)

            #insert musical work
            return  self.db.worksCollection.insert_one(musical_work)

        else:
            #reconcile musical works with same iswc
            reconcilied_work = self._reconcile_works(queryResult, musical_work)
            return self.db.worksCollection.replace_one({'_id': queryResult.get('_id')}, reconcilied_work)

    def retrieve_all_works(self):
        works = []

        for work in self.db.worksCollection.find():
            del work['_id']
            works.append(work)

        return works

    def retrieve_iswc_works(self):
        allWorks = []

        for work in self.db.worksCollection.find({'iswc': {"$ne": ''}}):
            del work['_id']
            allWorks.append(work)

        return allWorks

    def find_work_by_iswc(self, iswc):

        work = self.db.worksCollection.find_one({'iswc': iswc})
        if work is not None:
            del work['_id']

        return work
