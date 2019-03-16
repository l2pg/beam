import os
import uuid
import pandas as pd
from azure.storage.blob import AppendBlobService, BlockBlobService, PublicAccess

class FileStorage(object):
    def __init__(self, container_name):
        self.account_name = 'moremmrparsingstorage'
        self.account_key = 'UxvGtW/HsW83YcYWBa2IF3TCkr5R3zy4F72KxWy/POvzHPZVJwshZTihElfwHBGuoR2lfgGKcHdAh858IhqbjA=='
        self.container_name = container_name
        self.blob_service = None
        self.append_blob_service = None

    def connect_blob_service(self, reconnect=False):
        if type(self.blob_service).__name__ != 'BlockBlobService' or reconnect:
            try:
                self.blob_service = BlockBlobService(account_name=self.account_name,
                                                     account_key=self.account_key)
                self.blob_service.set_container_acl(self.container_name, public_access=PublicAccess.Container)
            except Exception as ex:
                print('Exception in connection to blob service: {0}'.format(ex))
                return False

        return True

    def connect_append_blob_service(self, reconnect=False):
        if type(self.append_blob_service).__name__ != 'AppendBlobService' or reconnect:
            try:
                self.append_blob_service = AppendBlobService(account_name=self.account_name,
                                                             account_key=self.account_key)
            except Exception as ex:
                print('Exception in connection to append blob service: {0}'.format(ex))
                return False

        return True

    def get_file(self, blob_name, result_file_path):
        blob_service_connected = self.connect_blob_service()

        if not blob_service_connected:
            return False
        else:
            try:
                self.blob_service.get_blob_to_path(container_name=self.container_name,
                                                   blob_name=blob_name,
                                                   file_path=result_file_path)
            except Exception as ex:
                if ex.error_code != 'BlobNotFound':
                    print('Getting file from blob exception: {0}:'.format(ex))

                return False

        return True

    def put_file(self, blob_name, local_file_path, remove_local_file_after=False):
        blob_service_connected = self.connect_blob_service()

        if not blob_service_connected:
            return False
        else:
            try:
                self.blob_service.create_blob_from_path(container_name=self.container_name,
                                                        blob_name=blob_name,
                                                        file_path=local_file_path)
            except Exception as ex:
                print('Putting file to blob exception: {0}:'.format(ex))
                return False

        if remove_local_file_after:
            try:
                os.remove(local_file_path)
            except OSError:
                pass

        return True

    def append_from_file(self, blob_name, local_file_path, remove_local_file_after=False):
        blob_service_connected = self.connect_append_blob_service()

        if not blob_service_connected:
            return False
        else:
            try:
                self.append_blob_service.append_blob_from_path(container_name=self.container_name,
                                                               blob_name=blob_name,
                                                               file_path=local_file_path)
            except Exception as ex:
                print('Append from file in blob exception: {0}:'.format(ex))
                return False

        if remove_local_file_after:
            try:
                os.remove(local_file_path)
            except OSError:
                pass

        return True

    def create_appended_blob(self, blob_name):
        blob_service_connected = self.connect_append_blob_service()

        if not blob_service_connected:
            return False
        else:
            try:
                self.append_blob_service.create_blob(container_name=self.container_name,
                                                     blob_name=blob_name)
            except Exception as ex:
                print('Creating appended file in blob exception: {0}:'.format(ex))
                return False

        return True

    def exists(self, blob_name):
        blob_service_connected = self.connect_blob_service()
        exists = False

        if blob_service_connected:
            try:
                exists = self.blob_service.exists(container_name=self.container_name, blob_name=blob_name)
            except Exception as ex:
                print('Checking file existence in blob exception: {0}:'.format(ex))

        return exists

    def delete_blob(self, blob_name):
        blob_service_connected = self.connect_blob_service()

        if blob_service_connected:
            try:
                self.blob_service.delete_blob(container_name=self.container_name,
                                              blob_name=blob_name)
            except Exception as ex:
                print('Deleting blob exception: {0}:'.format(ex))
                return False

        return True

    def put_array_to_blob(self, blob_name, array=[], compression=True):
        res = False

        local_file = '/tmp/{0}_{1}'.format(str(uuid.uuid4()), blob_name)

        try:
            df = pd.DataFrame(array)
        except Exception:
            df = pd.DataFrame()

        if df.shape[0] > 0:
            df.to_csv(path_or_buf=local_file,
                      header=False,
                      index=False,
                      compression=(None, 'zip')[compression])

            res = self.put_file(blob_name, local_file, True)

        return res

    def get_blob_as_df(self, blob_name, names=[]):
        df = pd.DataFrame()

        local_file = '/tmp/{0}_{1}'.format(str(uuid.uuid4()), blob_name)

        if self.get_file(blob_name, local_file):
            try:
                df = pd.read_csv(filepath_or_buffer=local_file, header=None, names=names)
            except Exception:
                df = pd.DataFrame()

        try:
            os.remove(local_file)
        except OSError:
            pass

        return df

    def append_df_to_blob(self, blob_name, df):
        local_file = '/tmp/{0}_{1}'.format(str(uuid.uuid4()), blob_name)

        df.to_csv(path_or_buf=local_file, header=None, index=False)

        if not self.exists(blob_name):
            self.create_appended_blob(blob_name)

        self.append_from_file(blob_name, local_file, True)

        return True

    def list_blobs(self, num_results=5000):
        blobs = []

        blob_service_connected = self.connect_blob_service()

        if blob_service_connected:
            try:
                blobs = self.blob_service.list_blobs(self.container_name,
                                                     num_results=num_results)
            except Exception as ex:
                print('List blobs exception: {0}'.format(ex))

        return blobs

