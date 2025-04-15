from shutil import copyfile

import bz2
import gzip
import configparser
import os
import shutil

import xml.etree.ElementTree

from pyreposync.sync_generic import SyncGeneric

from pyreposync.exceptions import OSRepoSyncException


class SyncRPM(SyncGeneric):
    def __init__(
        self,
        base_url,
        destination,
        reponame,
        date,
        treeinfo,
        proxy=None,
        client_cert=None,
        client_key=None,
        ca_cert=None,
    ):
        super().__init__(
            base_url,
            destination,
            reponame,
            date,
            proxy,
            client_cert,
            client_key,
            ca_cert,
        )
        self._treeinfo = treeinfo

    @property
    def treeinfo(self):
        return self._treeinfo

    def packages(self, base_path=None):
        if not base_path:
            base_path = f"{self.destination}/sync/{self.reponame}"
        primary = None
        for location, hash_algo, hash_sum in self.repomd_files(base_path=base_path):
            destination = f"{base_path}/{location}"
            if "primary.xml" in destination.lower():
                primary = destination
        if not primary:
            self.log.fatal("no primary.xml found in repomd.xml")
            raise OSRepoSyncException("no primary.xml found in repomd.xml")

        if primary.endswith(".gz"):
            with gzip.open(primary, "rb") as source:
                root = xml.etree.ElementTree.parse(source).getroot()
        elif primary.endswith("bz2"):
            with bz2.open(primary, "rb") as source:
                root = xml.etree.ElementTree.parse(source).getroot()
        else:
            with open(primary, "rb") as source:
                root = xml.etree.ElementTree.parse(source).getroot()
        packages = root.findall("{http://linux.duke.edu/metadata/common}package")
        for package in packages:
            checksum = package.find("{http://linux.duke.edu/metadata/common}checksum")
            hash_algo = checksum.get("type")
            hash_sum = checksum.text
            location = package.find("{http://linux.duke.edu/metadata/common}location")
            yield location.get("href"), hash_algo, hash_sum

    def migrate(self):
        migrated_file = f"{self.destination}/sync/{self.reponame}/migrated"
        if os.path.isfile(migrated_file):
            self.log.info("migration already done")
            return

        for location, hash_algo, hash_sum in self.packages():
            destination_old = f"{self.destination}/sync/{self.reponame}/{location}"
            destination_new = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
            try:
                os.rename(destination_old, destination_new)
            except FileNotFoundError:
                self.log.error(
                    f"could not migrate {location}: {destination_old} not found"
                )
                continue
            except OSError as err:
                self.log.error(f"could not migrate {location}: {err}")
                continue

        for snap in self.snap_list_timestamp_snapshots():
            self.log.info(f"migrating {snap}")
            base_path = f"{self.destination}/snap/{self.reponame}/{snap}"
            for location, hash_algo, hash_sum in self.packages(base_path=base_path):
                dst = f"{base_path}/{location}"
                src = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
                try:
                    os.unlink(dst)
                    os.symlink(src, dst)
                except OSError:
                    pass
                try:
                    os.symlink(src, dst)
                except OSError:
                    pass

        with open(migrated_file, "w") as _migrated:
            _migrated.write("migrated\n")

    def sync_packages(self):
        for location, hash_algo, hash_sum in self.packages():
            url = f"{self.base_url}{location}"
            destination = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=False)

    def treeinfo_files(self):
        treeinfo_file = f"{self.destination}/sync/{self.reponame}/{self.treeinfo}"
        treeinfo = configparser.ConfigParser()
        treeinfo.optionxform = str
        try:
            treeinfo.read_file(open(treeinfo_file))
        except FileNotFoundError:
            return
        try:
            for file in treeinfo.options("checksums"):
                if file == "repodata/repomd.xml":
                    continue
                hash_algo, hash_sum = treeinfo.get("checksums", file).split(":", 1)
                yield file, hash_algo, hash_sum
        except configparser.NoSectionError:
            files = set()
            for section in treeinfo.sections():
                if section.startswith("images-") or section.startswith("stage2"):
                    for option in treeinfo.options(section):
                        files.add(treeinfo.get(section, option))
            for file in files:
                yield file, None, None

    def sync_treeinfo(self):
        url = f"{self.base_url}{self.treeinfo}"
        destination = f"{self.destination}/sync/{self.reponame}/{self.treeinfo}"
        try:
            self.downloader.get(url, destination, replace=True)
        except OSRepoSyncException:
            return
        for file, hash_algo, hash_sum in self.treeinfo_files():
            if file == "repodata/repomd.xml":
                continue
            url = f"{self.base_url}{file}"
            destination = f"{self.destination}/sync/{self.reponame}/{file}"
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=True)

    def repomd_files(self, base_path=None):
        if not base_path:
            base_path = f"{self.destination}/sync/{self.reponame}"
        base_path = f"{base_path}/repodata/repomd.xml"
        repomd = xml.etree.ElementTree.parse(base_path).getroot()
        datas = repomd.findall("{http://linux.duke.edu/metadata/repo}data")
        for data in datas:
            checksum = data.find("{http://linux.duke.edu/metadata/repo}checksum")
            hash_algo = checksum.get("type")
            hash_sum = checksum.text
            location = data.find("{http://linux.duke.edu/metadata/repo}location")
            yield location.get("href"), hash_algo, hash_sum

    def revalidate(self):
        packages = dict()
        try:
            for location, hash_algo, hash_sum in self.packages():
                destination = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
                packages[destination] = {"hash_algo": hash_algo, "hash_sum": hash_sum}
        except FileNotFoundError:
            self.log.error("no repodata found")
        return packages

    def sync_repomd(self):
        url = f"{self.base_url}repodata/repomd.xml"
        destination = f"{self.destination}/sync/{self.reponame}/repodata/repomd.xml"
        try:
            shutil.rmtree(f"{self.destination}/sync/{self.reponame}/repodata/")
        except FileNotFoundError:
            pass
        self.downloader.get(url, destination, replace=True)
        for location, hash_algo, hash_sum in self.repomd_files():
            url = f"{self.base_url}{location}"
            destination = f"{self.destination}/sync/{self.reponame}/{location}"
            self.downloader.get(url, destination, hash_sum, hash_algo, replace=True)
        self.sync_packages()
        self.sync_treeinfo()

    def _snap(self):
        self.snap_repodata()
        self.snap_treeinfo()
        self.snap_packages()

    def snap_repodata(self):
        self.log.info("copy repodata")
        repomd_dst = (
            f"{self.destination}/snap/{self.reponame}/{self.date}/repodata/repomd.xml"
        )
        repomd_src = f"{self.destination}/sync/{self.reponame}/repodata/repomd.xml"
        try:
            os.makedirs(os.path.dirname(repomd_dst))
        except OSError:
            pass
        copyfile(repomd_src, repomd_dst)
        for location, hash_algo, hash_sum in self.repomd_files():
            dst = f"{self.destination}/snap/{self.reponame}/{self.date}/{location}"
            src = f"{self.destination}/sync/{self.reponame}/{location}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            copyfile(src, dst)
        self.log.info("done copy repodata")

    def snap_treeinfo(self):
        self.log.info("copy treeinfo")
        try:
            dst = f"{self.destination}/snap/{self.reponame}/{self.destination}/{self.treeinfo}"
            src = f"{self.destination}/sync/{self.reponame}/{self.treeinfo}"
            copyfile(src, dst)
        except (OSError, FileNotFoundError) as err:
            self.log.error(f"could not copy {self.treeinfo}: {err}")
        for location, hash_algo, hash_sum in self.treeinfo_files():
            dst = (
                f"{self.destination}/snap/{self.reponame}/{self.destination}/{location}"
            )
            src = f"{self.destination}/sync/{self.reponame}/{location}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            copyfile(src, dst)
        self.log.info("done copy treeinfo")

    def snap_packages(self):
        self.log.info("copy packages")
        for location, hash_algo, hash_sum in self.packages():
            dst = f"{self.destination}/snap/{self.reponame}/{self.date}/{location}"
            src = f"{self.destination}/sync/{self.reponame}/{location}.{hash_algo}.{hash_sum}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            try:
                os.symlink(src, dst)
            except FileExistsError as err:
                self.log.error(f"could not copy {location}: {err}")
        self.log.info("done copy packages")

    def sync(self):
        self.log.info("starting thread")
        self.sync_repomd()
        self.log.info("shutdown thread complete")
