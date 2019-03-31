import psycopg2
import psycopg2.extras
import __builtin__


class PostgresDb(object):
    def __init__(self):
        self.postgres_db = None

        self.DSN = 'dbname={0} user={1} password={2} host={3} port={4}'.format(os.environ['POSTGRES_DB'],
                                                                               os.environ['POSTGRES_USER'],
                                                                               os.environ['POSTGRES_PWD'],
                                                                               os.environ['POSTGRES_HOST'],
                                                                               os.environ['POSTGRES_PORT'])

    def __del__(self):
        self.close_connection()

    def connect_to_postgres_db(self):
        need_reconnection = False
        if type(self.postgres_db).__name__ == 'connection':
            if self.postgres_db.closed == 1:
                need_reconnection = True
        else:
            need_reconnection = True

        if need_reconnection:
            try:
                self.postgres_db = psycopg2.connect(self.DSN)
                cursor = self.postgres_db.cursor()
                cursor.execute('select 1')
                if cursor.rowcount != 1:
                    # try to connect again
                    self.postgres_db = psycopg2.connect(self.DSN)
                    cursor = self.postgres_db.cursor()
                    cursor.execute('select 1')
                    if cursor.rowcount != 1:
                        print('PostgreSQL pre-ping failed !')
                        return False
                self.postgres_db.set_client_encoding('UTF8')
            except Exception as ex:
                print('PostgreSQL connection failed: {0}'.format(ex))
                return False

        return True

    def close_connection(self):
        if type(self.postgres_db).__name__ == 'connection':
            self.postgres_db.close()
        self.postgres_db = None

    def select_pure(self, sql_str, close_connection=True):
        rows = []
        if self.connect_to_postgres_db():
            cursor = self.postgres_db.cursor()
            try:
                cursor.execute(sql_str)

                if cursor.rowcount > 0:
                    rows = cursor.fetchall()

                cursor.close()
            except Exception as ex:
                print('SELECT failed: {0} -> {1}'.format(sql_str, ex))

        if close_connection:
            self.close_connection()

        return rows

    def update_by_delete_insert_with_df(self, table_name, df, **kwargs):
        res = False

        part_col_name = kwargs.get("partition_column_name", "")
        part_col_type = kwargs.get("partition_column_type", str)
        key_col_name = kwargs.get("key_column_name", "")
        key_col_type = kwargs.get("key_column_type", str)

        del_stm = "delete from {} where 1=1".format(table_name)

        if len(df) > 0:
            part_keys = []
            del_keys = []

            if part_col_name:
                part_keys = list(set(df[part_col_name].astype(part_col_type).tolist()))

                if len(part_keys) > 0:
                    del_stm = "{0} and {1} = any(%s)".format(del_stm, part_col_name)

            if key_col_name:
                del_keys = list(set(df[key_col_name].astype(key_col_type).tolist()))
                del_stm = "{0} and {1} = any(%s)".format(del_stm, key_col_name)

            df_columns = list(df)
            ins_columns = ",".join(df_columns)
            ins_values = "values({})".format(",".join(["%s" for _ in df_columns]))
            ins_stm = "insert into {0}({1}) {2}".format(table_name,
                                                        ins_columns,
                                                        ins_values)

            if self.connect_to_postgres_db():
                cursor = self.postgres_db.cursor()
                try:
                    # delete if partition key or deleting keys are provided
                    part_keys_exist = len(part_keys) > 0
                    del_keys_exist = len(del_keys) > 0

                    if part_keys_exist or del_keys_exist:
                        exec_values = (part_keys, del_keys) \
                            if part_keys_exist and del_keys_exist \
                            else (tuple([del_keys]) if del_keys_exist
                                  else tuple([part_keys]))
                        cursor.execute(del_stm, exec_values)

                    # insert
                    psycopg2.extras.execute_batch(cursor, ins_stm, df.values)
                    self.postgres_db.commit()
                    res = True

                except Exception as ex:
                    print('Update_by_delete_insert_with_df failed with exception {0}'.format(ex))

                cursor.close()
                self.close_connection()

        return res

    def update_tgt_by_tmp(self, tmp_table='', tgt_table='', columns=[]):
        res = False

        if tmp_table and tgt_table and columns:
            sql_str = 'insert into {0}({1})' \
                      'select {1} ' \
                      'from {2} ' \
                      'returning 1'.format(tgt_table,
                                           ','.join(columns),
                                           tmp_table)

            if self.connect_to_postgres_db():
                cursor = self.postgres_db.cursor()

                try:
                    cursor.execute(sql_str)

                    if cursor.rowcount > 0:
                        self.postgres_db.commit()
                        res = True
                except Exception as ex:
                    print('Update_tgt_by_tmp failed with exception {0}'.format(ex))

                cursor.close()
                self.close_connection()

        return res

    def truncate_table_by_delete(self, table_name):
        res = False

        if table_name:
            del_stm = 'delete from {0}'.format(table_name)

            if self.connect_to_postgres_db():
                cursor = self.postgres_db.cursor()

                try:
                    cursor.execute(del_stm)
                    self.postgres_db.commit()
                    res = True
                except Exception as ex:
                    print('Truncate_table_by_delete failed with exception {0}'.format(ex))

                cursor.close()
                self.close_connection()

        return res

    def insert_into_table_with_df(self, table_name, df, close_connection=True):
        res = False

        df_columns = list(df)
        ins_columns = ",".join(df_columns)
        ins_values = "values({})".format(",".join(["%s" for _ in df_columns]))
        ins_stm = "insert into {0}({1}) {2} returning 1".format(table_name, ins_columns, ins_values)

        if self.connect_to_postgres_db():
            cursor = self.postgres_db.cursor()
            try:
                psycopg2.extras.execute_batch(cursor, ins_stm, df.values)
                if cursor.rowcount > 0:
                    self.postgres_db.commit()
                    res = True

            except Exception as ex:
                print('Insert_into_table_with_df failed with exception {0}'.format(ex))

            cursor.close()

            if close_connection:
                self.close_connection()

        return res

    def get_kv_settings(self, table_name):
        select_stm = 'select key, value, type from {0}'.format(table_name)

        settings = {}

        if self.connect_to_postgres_db():
            cursor = self.postgres_db.cursor()
            try:
                cursor.execute(select_stm)

                if cursor.rowcount > 0:
                    rows = cursor.fetchall()
                    for row in rows:
                        key = row[0]
                        val = row[1]
                        value_type = row[2]

                        try:
                            if value_type == 'bool':
                                value = True if str(val).lower() in ('1','true') else False
                            else:
                                value = getattr(__builtin__, value_type)(val)
                        except (TypeError, ValueError):
                            value = str(val)

                        settings[key] = value
            except Exception as ex:
                print('Get_kv_settings failed with exception {0}'.format(ex))

            cursor.close()
            self.close_connection()

        return settings

    def run_sql_commit_if_returns(self, sql_stm='', close_connection=True):
        res = False
        rows = []

        if sql_stm:
            if self.connect_to_postgres_db():
                cursor = self.postgres_db.cursor()
                try:
                    cursor.execute(sql_stm)
                    if cursor.rowcount > 0:
                        self.postgres_db.commit()
                        rows = cursor.fetchall()
                        res = True
                except Exception as ex:
                    print('Run_sql_commit_if_returns failed with exception {0}'.format(ex))

                cursor.close()

                if close_connection:
                    self.close_connection()

        return res, rows

