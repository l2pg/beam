import apache_beam as beam

class DoFnBase(beam.DoFn):
    def __init__(self, **kwargs):
        super(DoFnBase, self).__init__()
        self.arguments = {}
        self.files_collector = set()
        self.file_storage = kwargs.get('file_storage', None)
        self.pg_db = kwargs.get('pg_db', None)
        for key, val in kwargs.items():
            if key in ['file_storage', 'pg_db']:
                continue
            self.arguments[key] = val

    def __del__(self):
        self.drop_tmp_files()

    @property
    def pg_db(self):
        return self.__pg_db

    @property
    def file_storage(self):
        return self.__file_storage

    @pg_db.setter
    def pg_db(self, val):
        self.__pg_db = val

    @file_storage.setter
    def file_storage(self, val):
        self.__file_storage = val

    def reg_tmp_file(self, file_path):
        self.files_collector.add(file_path)
        return file_path

    def drop_tmp_files(self):
        import os
        for file_name in self.files_collector:
            try:
                os.remove(file_name)
            except OSError:
                pass

    def get_argument(self, name, default=None):
        arg_val = self.arguments.get(name, None)
        if arg_val is None:
            arg_val = default
        return arg_val

    def get_blob_as_df(self, blob_name, names=[]):
        df = None

        if self.file_storage:
            df = self.file_storage.get_blob_as_df(blob_name=blob_name, names=names)

        return df

    def push_array_to_blob(self, blob_name, array, compression=True):
        res = False

        if self.file_storage:
            res = self.file_storage.put_array_to_blob(blob_name=blob_name, array=array, compression=compression)

        return res

    def append_df_to_blob(self, blob_name, df):
        res = False
        if self.file_storage:
            res = self.file_storage.append_df_to_blob(blob_name=blob_name, df=df)
        return res

    def update_by_delete_insert_with_df(self, table_name, df, **kwargs):
        res = False
        if self.pg_db:
            res = self.pg_db.update_by_delete_insert_with_df(table_name, df, **kwargs)
        return res

    def delete_blob(self, blob_name):
        res = False
        if self.file_storage:
            res = self.file_storage.delete_blob(blob_name=blob_name)
        return res
