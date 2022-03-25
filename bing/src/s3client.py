class S3Client:
    def __init__(self, bucket_name=None, path=None):
        self.bucket_name = bucket_name
        self.path = path
