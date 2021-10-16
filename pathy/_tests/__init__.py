has_gcs: bool
try:
    from ..gcs import BucketClientGCS

    has_gcs = bool(BucketClientGCS)
except ImportError:
    has_gcs = False

has_s3: bool
try:
    from ..s3 import BucketClientS3

    has_s3 = bool(BucketClientS3)
except ImportError:
    has_s3 = False

has_azure: bool
try:
    from ..azure import BucketClientAzure

    has_azure = bool(BucketClientAzure)
except ImportError:
    has_azure = False
