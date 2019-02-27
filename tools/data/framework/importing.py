import logging


class ImportingError(Exception):
    pass


class Importing(object):
    pass
    # def __init__(self, databus, report_db, model_meta, logger=logging.getLogger('propellerads')):
    #     self._databus = databus
    #     self._report_db = report_db
    #     self._meta = model_meta
    #     self.log = logger
    #
    # def create_entity_table(self, entity_name):
    #     table_name = self._entity_name_to_table_name(entity_name)
    #     sql = """
    #     CREATE TABLE IF NOT EXISTS %s.%s
    #     (
    #         date_added Date DEFAULT today(),
    #         id UInt64
    #     ) ENGINE = MergeTree(date_added, (id, date_added), 8192)
    #     """ % (self._report_db._db, table_name)
    #     self.write(sql)
    #
    # def _entity_name_to_table_name(self, entity_name):
    #     return "%s" % (entity_name.lower())
    #
    # def init_scheme(self):
    #     pass
    #     # for entity_name in self._meta.keys():
    #     #     self._build_scheme_for(entity_name)
    #
    # def run(self):
    #     pass
    #     # self.init_scheme()
