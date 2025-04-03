import hashlib
import logging
import os
import requests
import requests.exceptions
import shutil
import tempfile
import time

from pyreposync.exceptions import OSRepoSyncDownLoadError, OSRepoSyncHashError


class Downloader(object):
    def __init__(self, proxy=None, client_cert=None, client_key=None, ca_cert=None):
        self.log = logging.getLogger("application")
        if proxy:
            self._proxy = {"http": proxy, "https": proxy}
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
        if hash_type == "md5":
            hasher = hashlib.md5()
        elif hash_type == "sha":
            hasher = hashlib.sha1()
        elif hash_type == "sha1":
            hasher = hashlib.sha1()
        elif hash_type == "sha256":
            hasher = hashlib.sha256()
        elif hash_type == "sha512":
            hasher = hashlib.sha512()

        with open(destination, "rb") as dest:
            hasher.update(dest.read())
            self.log.debug(f"expected hash: {hasher.hexdigest()}")
            self.log.debug(f"actual hash: {checksum}")
            if hasher.hexdigest() == checksum:
                self.log.debug(f"download valid: {destination}")
            else:
                self.log.error(f"download invalid: {destination}")
                raise OSRepoSyncHashError(f"download invalid: {destination}")

    def get(
        self,
        url,
        destination,
        checksum=None,
        hash_type=None,
        replace=False,
        not_found_ok=False,
    ):
        self.log.info(f"downloading: {url}")
        if not replace:
            if os.path.isfile(destination):
                self.log.info("already there, not downloading")
                return
        retries = 10
        while retries >= 0:
            try:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    tmp_file = os.path.join(tmp_dir, os.path.basename(destination))
                    self._get(url, tmp_file, checksum, hash_type, not_found_ok)
                    self.create_dir(destination)
                    try:
                        shutil.move(tmp_file, destination)
                    except OSError:
                        if not_found_ok:
                            pass
                        else:
                            raise
                self.log.info(f"done downloading: {url}")
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
        self.log.error(f"could not download: {url}")
        raise OSRepoSyncDownLoadError(f"could not download: {url}")

    def create_dir(self, destination):
        if not os.path.isdir(os.path.dirname(destination)):
            try:
                os.makedirs(os.path.dirname(destination))
            except OSError as err:
                self.log.error(f"could not create directory: {err}")
                raise OSRepoSyncDownLoadError(f"could not create directory: {err}")

    def _get(
        self,
        url,
        destination,
        checksum=None,
        hash_type=None,
        not_found_ok=False,
    ):
        self.create_dir(destination)
        r = requests.get(
            url, stream=True, proxies=self.proxy, cert=self.cert, verify=self.ca_cert
        )
        if r.status_code == 200:
            with open(destination, "wb", 0) as dst:
                r.raw.decode_content = True
                shutil.copyfileobj(r.raw, dst)
                dst.flush()
        else:
            if not_found_ok:
                if r.status_code == 404:
                    self.log.info("not found, skipping")
                    return
            raise OSRepoSyncDownLoadError()
        if checksum:
            self.check_hash(
                destination=destination, checksum=checksum, hash_type=hash_type
            )
        self.log.info(f"successfully fetched: {url}")
