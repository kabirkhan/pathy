import builtins
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional

from azure.core.paging import ItemPaged

try:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobProperties
except (ImportError, ModuleNotFoundError):
    raise ImportError(
        """You are using the Azure functionality of Pathy without
    having the required dependencies installed.

    Please try installing them:

        pip install pathy[azure]

    """
    )

from . import (
    Blob,
    Bucket,
    BucketClient,
    BucketEntry,
    PathyScanDir,
    PurePathy,
    register_client,
)


class BucketEntryAzure(BucketEntry):
    bucket: "BucketAzure"
    raw: Any  # type:ignore[override]


@dataclass
class BlobAzure(Blob):
    def delete(self) -> None:
        self.raw.delete_blob()  # type:ignore

    def exists(self) -> bool:
        print("CALLING EXISTS:", self.raw)
        return self.raw.exists()  # type:ignore


@dataclass
class BucketAzure(Bucket):
    name: str
    bucket: ContainerClient

    def get_blob(self, blob_name: str) -> Optional[BlobAzure]:
        assert isinstance(
            blob_name, str
        ), f"expected str blob name, but found: {type(blob_name)}"
        native_blob_client: Optional[BlobClient] = self.bucket.get_blob_client(blob_name)  # type:ignore
        if native_blob_client is None or not native_blob_client.exists():
            return None
        properties: BlobProperties = native_blob_client.get_blob_properties()
        return BlobAzure(
            bucket=self.bucket,
            owner=None,  # type:ignore
            name=native_blob_client.blob_name,  # type:ignore
            raw=native_blob_client,
            size=properties.size,
            updated=int(properties.last_modified.timestamp())
        )

    # def copy_blob(  # type:ignore[override]
    #     self, blob: BlobAzure, target: "BucketAzure", name: str
    # ) -> Optional[BlobAzure]:
    #     assert blob.raw is not None, "raw storage.Blob instance required"
    #     container_client: ContainerClient = self.bucket
    #     container_client.get_blob_client(blob.name).
    #     native_blob:  = container_client  . (  # type: ignore
    #         blob.raw, target.bucket, name
    #     )
    #     return BlobAzure(
    #         bucket=self.bucket,
    #         owner=None,  # type:ignore
    #         name=native_blob.name,  # type:ignore
    #         raw=native_blob,
    #         size=native_blob.size,
    #         updated=int(native_blob.updated.timestamp()),  # type:ignore
    #     )

    def delete_blob(self, blob: BlobAzure) -> None:  # type:ignore[override]
        return self.bucket.delete_blob(blob.name)  # type:ignore

    def delete_blobs(self, blobs: List[BlobAzure]) -> None:  # type:ignore[override]
        if blobs:
            self.bucket.delete_blobs(blobs)  # type:ignore

    def exists(self) -> bool:
        return self.bucket.exists()  # type:ignore


@register_client("azure")
class BucketClientAzure(BucketClient):
    client: BlobServiceClient

    @property
    def client_params(self) -> Any:
        return dict(client=self.client)

    def __init__(self, **kwargs: Any) -> None:
        self.recreate(**kwargs)

    def recreate(self, **kwargs: Any) -> None:
        conn_str = kwargs.get("connection_string")
        account_url = kwargs.get("account_url")
        credential = kwargs.get("credential")
        if conn_str is not None:
            self.client = BlobServiceClient.from_connection_string(conn_str)
        else:
            self.client = BlobServiceClient(account_url=account_url, credential=credential)

    def make_uri(self, path: PurePathy) -> str:
        return str(path)

    def create_bucket(  # type:ignore[override]
        self, path: PurePathy
    ):
        return self.client.create_container(path.root)  # type:ignore

    def delete_bucket(self, path: PurePathy) -> None:
        self.client.delete_container(path.root)  # type:ignore

    def exists(self, path: PurePathy) -> bool:
        # Because we want all the parents of a valid blob (e.g. "directory" in
        # "directory/foo.file") to return True, we enumerate the blobs with a prefix
        # and compare the object names to see if they match a substring of the path
        key_name = str(path.key)
        print("CALLING EXISTS:", key_name, path)
        for obj in self.list_blobs(path):
            if obj.name.startswith(key_name + path._flavour.sep):  # type:ignore
                return True
        return False

    def lookup_bucket(self, path: PurePathy) -> Optional[BucketAzure]:
        try:
            return self.get_bucket(path)
        except FileNotFoundError:
            return None

    def get_bucket(self, path: PurePathy) -> BucketAzure:
        container_client: ContainerClient = self.client.get_container_client(path.root)
        try:
            if container_client.exists():
                return BucketAzure(str(path.root), bucket=container_client)
        except Exception as e:# BadRequest: TODO
            print("ERROR GETTING BUCKET")
            print(e)
            pass
        raise FileNotFoundError(f"Bucket {path.root} does not exist!")

    def list_buckets(  # type:ignore[override]
        self, **kwargs: Dict[str, Any]
    ) -> Generator[Any, None, None]:
        return self.client.list_containers(**kwargs)  # type:ignore

    def scandir(  # type:ignore[override]
        self,
        path: Optional[PurePathy] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> PathyScanDir:
        return ScanDirAzure(client=self, path=path, prefix=prefix, delimiter=delimiter)

    def list_blobs(
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> Generator[BlobAzure, None, None]:
        bucket = self.lookup_bucket(path)
        if bucket is None:
            return

        container_client: ContainerClient = self.client.get_container_client(path.root)
        container_client.list_blobs()
        response: ItemPaged[BlobProperties] = container_client.list_blobs(  # type:ignore
            path.root, prefix=prefix
        )
        
        for item in response:  # type:ignore
            native_blob_client = container_client.get_blob_client(item.name)
            yield BlobAzure(
                bucket=container_client.container_name,
                owner=None,
                name=item.name,
                raw=native_blob_client,
                size=item.size,
                updated=int(item.last_modified.timestamp()),
            )


class ScanDirAzure(PathyScanDir):
    _client: BucketClientAzure

    def __init__(
        self,
        client: BucketClient,
        path: Optional[PurePathy] = None,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> None:
        super().__init__(client=client, path=path, prefix=prefix, delimiter=delimiter)
        self._page_size = page_size

    def scandir(self) -> Generator[BucketEntryAzure, None, None]:
        if self._path is None or not self._path.root:
            azure_bucket: BucketAzure
            for azure_bucket in self._client.list_buckets():
                yield BucketEntryAzure(azure_bucket.name, is_dir=True, raw=None)
            return
        sep = self._path._flavour.sep  # type:ignore
        bucket = self._client.lookup_bucket(self._path)
        if bucket is None:
            return

        container_client: ContainerClient = self._client.client.get_container_client(self._path.root)
        for file in container_client.walk_blobs('', delimiter='/'):
            yield BucketEntryAzure(
                name=file.name
            )
