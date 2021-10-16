import pytest

from pathy import Pathy, get_client, use_fs

from . import has_azure
from .conftest import ENV_ID

AZURE_ADAPTER = ["azure"]


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not has_azure, reason="requires azure")
def test_azure_create_bucket(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    root = Pathy(f"azure://foo")
    root.mkdir(exist_ok=True)
    client = root._accessor.client(root)
    assert "foo" in [bucket.name for bucket in client.list_buckets()]
    root.rmdir()
    assert "foo" not in [bucket.name for bucket in client.list_buckets()]


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not has_azure, reason="requires azure")
def test_azure_create_blob(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    path: Pathy = Pathy(f"{with_adapter}://{bucket}/to_local")
    path.mkdir(exist_ok=True)
    foo_blob: Pathy = path / "foo"
    foo_blob.write_text("---")
    assert isinstance(foo_blob, Pathy)
    assert foo_blob.exists()

    assert 0


@pytest.mark.parametrize("adapter", AZURE_ADAPTER)
@pytest.mark.skipif(not has_azure, reason="requires azure")
def test_azure_list_buckets(
    with_adapter: str, bucket: str, other_bucket: str
) -> None:
    from pathy.azure import BucketClientAzure

    root = Pathy(f"azure://foo/bar")
    client = root._accessor.client(root)  # type:ignore
    buckets = client.list_buckets()
    print(list(buckets))

    assert 0
    # scandir = ScanDirS3(client=client, path=Pathy())
    # buckets = [s.name for s in scandir]
    # assert bucket in buckets
    # assert other_bucket in buckets
