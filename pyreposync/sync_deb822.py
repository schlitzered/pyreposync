from shutil import copyfile

import gzip
import os

from pyreposync.sync_generic import SyncGeneric


class SyncDeb822(SyncGeneric):
    def __init__(
        self,
        base_url,
        destination,
        reponame,
        date,
        suites,
        components,
        binary_archs,
        allow_missing_packages,
        basic_auth_user=None,
        basic_auth_pass=None,
        proxy=None,
        client_cert=None,
        client_key=None,
        ca_cert=None,
    ):
        super().__init__(
            base_url=base_url,
            destination=destination,
            reponame=reponame,
            date=date,
            allow_missing_packages=allow_missing_packages,
            basic_auth_user=basic_auth_user,
            basic_auth_pass=basic_auth_pass,
            proxy=proxy,
            client_cert=client_cert,
            client_key=client_key,
            ca_cert=ca_cert,
        )
        self._binary_archs = binary_archs
        self._components = components
        self._destination = destination
        self._suites = suites

    @property
    def binary_archs(self) -> list[str]:
        return self._binary_archs

    @property
    def suites(self) -> list[str]:
        return self._suites

    @property
    def components(self) -> list[str]:
        return self._components

    def _snap(self):
        for suite in self.suites:
            self.snap_suites(suite=suite)

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
        self.log.info(
            f"creating snapshot for suite {suite} arch {arch} package binary files"
        )
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
