from shutil import copyfile

import bz2
import gzip
import logging
import os
import shutil

from pyreposync.downloader import Downloader
from pyreposync.exceptions import OSRepoSyncException, OSRepoSyncHashError


class DEBSync:
    def __init__(
        self,
        base_url,
        destination,
        reponame,
        date,
        suites,
        components,
        binary_archs,
        proxy=None,
        client_cert=None,
        client_key=None,
        ca_cert=None,
    ):
        self._binary_archs = binary_archs
        self._base_url = base_url
        self._components = components
        self._date = date
        self._destination = destination
        self._reponame = reponame
        self._suites = suites
        self.downloader = Downloader(
            proxy=proxy, client_cert=client_cert, client_key=client_key, ca_cert=ca_cert
        )
        self.log = logging.getLogger("application")

    @property
    def binary_archs(self) -> list[str]:
        return self._binary_archs

    @property
    def base_url(self):
        return self._base_url

    @property
    def date(self):
        return self._date

    @property
    def destination(self):
        return self._destination

    @property
    def reponame(self):
        return self._reponame

    @property
    def suites(self) -> list[str]:
        return self._suites

    @property
    def components(self) -> list[str]:
        return self._components

    def migrate(self):
        pass

    def snap(self):
        self.log.info("creating snapshot")
        for suite in self.suites:
            self.snap_suites(suite=suite)
        current = f"{self.destination}/snap/{self.reponame}/{self.date}"
        latest = f"{self.destination}/snap/{self.reponame}/latest"
        timestamp = f"{self.destination}/snap/{self.reponame}/{self.date}/timestamp"
        self.log.info("setting latest to current release")
        try:
            os.unlink(latest)
        except FileNotFoundError:
            pass
        os.symlink(current, latest)
        with open(timestamp, "w") as _timestamp:
            _timestamp.write(f"{self.destination}\n")
        self.log.info("done creating snapshot")

    def snap_suites(self, suite):
        self.log.info(f"creating snapshot for suite {suite}")
        self.snap_release(suite=suite)
        self.snap_release_files(suite=suite)
        for arch in self.binary_archs:
            self.snap_package_binary_files(suite=suite, arch=arch)
        self.log.info(f"creating snapshot for suite {suite}, done")

    def snap_release(self, suite):
        self.log.info(f"creating snapshot for suite {suite} release files")
        release_files = ["InRelease", "Release", "Release.gpg"]
        src_path = f"{self.destination}/sync/{self.reponame}/dists/{suite}"
        dst_path = f"{self.destination}/snap/{self.reponame}/{self.date}/dists/{suite}"
        try:
            os.makedirs(dst_path)
        except OSError:
            pass
        for release_file in release_files:
            src = f"{src_path}/{release_file}"
            dst = f"{dst_path}/{release_file}"
            copyfile(src, dst)
        self.log.info(f"creating snapshot for suite {suite} release files, done")

    def snap_release_files(self, suite):
        self.log.info(f"creating snapshot for suite {suite} release files")
        release_files = self.release_files_sha256(suite=suite)
        src_path = f"{self.destination}/sync/{self.reponame}/dists/{suite}"
        dst_path = f"{self.destination}/snap/{self.reponame}/{self.date}/dists/{suite}"
        for filename, sha256_dict in release_files.items():
            src = f"{src_path}/{filename}"
            dst = f"{dst_path}/{filename}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            try:
                copyfile(src, dst)
            except FileNotFoundError:
                pass
        self.log.info(f"creating snapshot for suite {suite} release files, done")

    def snap_package_binary_files(self, suite, arch):
        self.log.info(f"creating snapshot for suite {suite} arch {arch} package binary files")
        packages = self.binary_files_sha256(suite=suite, component="main", arch=arch)
        src_path = f"{self.destination}/sync/{self.reponame}"
        dst_path = f"{self.destination}/snap/{self.reponame}/{self.date}"
        for filename, sha256_dict in packages.items():
            src = f"{src_path}/{filename}.sha256.{sha256_dict['sha256']}"
            dst = f"{dst_path}/{filename}"
            try:
                os.makedirs(os.path.dirname(dst))
            except OSError:
                pass
            try:
                os.symlink(src, dst)
            except FileExistsError:
                pass

    def snap_name(self, timestamp, snapname):
        self.log.info("creating named snapshot")
        self.log.info("done creating named snapshot")

    def snap_unname(self, snapname):
        self.log.info("removing named snapshot")
        self.log.info("done removing named snapshot")

    def sync(self):
        self.log.info("starting thread")
        for suite in self.suites:
            self.sync_suites(suite=suite)
        self.log.info("shutdown thread complete")

    def sync_suites(self, suite):
        self.log.info(f"syncing suite {suite}")
        self.sync_release(suite=suite)
        self.sync_release_files(suite=suite)
        for arch in self.binary_archs:
            self.sync_package_binary_files(suite=suite, arch=arch)
        self.log.info(f"syncing suite {suite}, done")

    def sync_release(self, suite):
        self.log.info(f"syncing suite {suite} release files")
        release_files = ["InRelease", "Release", "Release.gpg"]
        base_path = f"{self.destination}/sync/{self.reponame}/dists/{suite}"
        base_url = f"{self.base_url}/dists/{suite}"
        self.log.info(base_url)
        for release_file in release_files:
            self.downloader.get(
                url=f"{base_url}/{release_file}",
                destination=f"{base_path}/{release_file}",
                replace=True,
            )
        self.log.info(f"syncing suite {suite} release files, done")

    def sync_package_binary_files(self, suite, arch):
        self.log.info(f"syncing suite {suite} arch {arch} package binary files")
        packages = self.binary_files_sha256(suite=suite, component="main", arch=arch)
        base_path = f"{self.destination}/sync/{self.reponame}"
        base_url = f"{self.base_url}"
        for filename, sha256_dict in packages.items():
            self.downloader.get(
                url=f"{base_url}/{filename}",
                destination=f"{base_path}/{filename}.sha256.{sha256_dict['sha256']}",
                checksum=sha256_dict["sha256"],
                hash_type="sha256",
            )

        self.log.info(f"syncing suite {suite} arch {arch} package binary files, done")

    def binary_files_sha256(self, suite, component, arch):
        packages_gz_file = f"{self.destination}/sync/{self.reponame}/dists/{suite}/{component}/binary-{arch}/Packages.gz"
        packages = dict()
        sha256 = None
        filename = None
        size = None
        with gzip.open(packages_gz_file, "rb") as source:
            for line in source:
                line = line.decode("utf-8")
                if line.startswith("SHA256: "):
                    sha256 = line.split("SHA256: ")[1].strip()
                elif line.startswith("Filename: "):
                    filename = line.split("Filename: ")[1].strip()
                elif line.startswith("Size: "):
                    size = int(line.split("Size: ")[1].strip())
                if filename and sha256 and size:
                    packages[filename] = {
                        "sha256": sha256,
                        "size": size,
                    }
                    sha256 = None
                    filename = None
                    size = None
        return packages

    def sync_release_files(self, suite):
        self.log.info(f"syncing suite {suite} release files")
        release_files = self.release_files_sha256(suite=suite)
        base_path = f"{self.destination}/sync/{self.reponame}/dists/{suite}"
        base_url = f"{self.base_url}/dists/{suite}"
        for filename, sha256_dict in release_files.items():
            self.downloader.get(
                url=f"{base_url}/{filename}",
                destination=f"{base_path}/{filename}",
                checksum=sha256_dict["sha256"],
                hash_type="sha256",
                replace=True,
                not_found_ok=True,
            )
        self.log.info(f"syncing suite {suite} release files, done")

    def release_files_sha256(self, suite):
        release = f"{self.destination}/sync/{self.reponame}/dists/{suite}/Release"
        with open(release, "r") as release_file:
            release_file_content = release_file.read()
        sha256_dict = {}
        in_sha256_section = False
        for line in release_file_content.splitlines():
            if line.startswith("SHA256:"):
                in_sha256_section = True
                continue
            if in_sha256_section:
                if line.startswith(" "):
                    sha256, size, filename = line.split()
                    sha256_dict[filename] = {
                        "sha256": sha256,
                        "size": int(size),
                    }
                else:
                    break
        return sha256_dict
