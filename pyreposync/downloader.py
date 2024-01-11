import hashlib
import logging
import os
import requests
import requests.exceptions
import shutil
import time

from pyreposync.exceptions import OSRepoSyncDownLoadError, OSRepoSyncHashError


class Downloader(object):
    def __init__(self, proxy=None, client_cert=None, client_key=None, ca_cert=None):
        self.log = logging.getLogger('application')
        if proxy:
            self._proxy = {
                'http': proxy,
                'https': proxy
            }
        else:
            self._proxy = None
        if client_cert and client_key:
            self._cert = (client_cert, client_key)
        else:
            self._cert = None
        if ca_cert:
            self._ca_cert = ca_cert
        else:
            self._ca_cert = True

    @property
    def ca_cert(self):
        return self._ca_cert

    @property
    def cert(self):
        return self._cert

    @property
    def proxy(self):
        return self._proxy

    def check_hash(self, destination, checksum, hash_type):
        self.log.debug("validating hash")
        hasher = None
        if hash_type == 'md5':
            hasher = hashlib.md5()
        elif hash_type == 'sha':
            hasher = hashlib.sha1()
        elif hash_type == 'sha1':
            hasher = hashlib.sha1()
        elif hash_type == 'sha256':
            hasher = hashlib.sha256()
        elif hash_type == 'sha512':
            hasher = hashlib.sha512()

        with open(destination, 'rb') as dest:
            hasher.update(dest.read())
            self.log.debug("expected hash: {0}".format(hasher.hexdigest()))
            self.log.debug("actual hash: {0}".format(checksum))
            if hasher.hexdigest() == checksum:
                self.log.debug("download valid: {0}".format(destination))
            else:
                self.log.error("download invalid: {0}".format(destination))
                raise OSRepoSyncHashError('download invalid: {0}'.format(destination))

    def get(self, url, destination, checksum=None, hash_type=None, replace=False):
        self.log.info("downloading: {0}".format(url))
        if not replace:
            if os.path.isfile(destination):
                self.log.info("already there, not downloading")
                return
        retries = 10
        while retries >= 0:
            try:
                self._get(url, destination, checksum, hash_type)
                self.log.info("done downloading: {0}".format(url))
                return
            except requests.exceptions.ConnectionError:
                self.log.error("could not fetch resource, retry in 10 seconds")
                retries -= 1
                time.sleep(10)
            except OSRepoSyncHashError:
                self.log.error("download invalid, retry in 10 seconds")
                retries -= 1
                time.sleep(10)
            except OSRepoSyncDownLoadError:
                break
        self.log.error("could not download: {0}".format(url))
        raise OSRepoSyncDownLoadError("could not download: {0}".format(url))

    def _get(self, url, destination, checksum=None, hash_type=None):
        if not os.path.isdir(os.path.dirname(destination)):
            try:
                os.makedirs(os.path.dirname(destination))
            except OSError:
                pass
        r = requests.get(url, stream=True, proxies=self.proxy, cert=self.cert, verify=self.ca_cert)
        if r.status_code == 200:
            with open(destination, 'wb', 0) as dst:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, dst)
                dst.flush()
        else:
            raise OSRepoSyncDownLoadError()
        if checksum:
            self.check_hash(destination=destination, checksum=checksum, hash_type=hash_type)
        self.log.info("successfully fetched: {0}".format(url))
