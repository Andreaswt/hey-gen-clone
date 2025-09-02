import os
import subprocess
import uuid

import modal
from pydantic import BaseModel

app = modal.App("sieve-file-to-s3")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install("curl")
    .pip_install_from_requirements("sieve-to-s3/requirements.txt")
)

s3_secret = modal.Secret.from_name("hey-gen-secret")


class SieveImportRequest(BaseModel):
    video_url: str


class SieveImportResponse(BaseModel):
    s3_key: str


@app.cls(
    image=image,
    volumes={
        "/s3-mount": modal.CloudBucketMount("hey-gen-clone", secret=s3_secret)
    },
    timeout=600,
    secrets=[s3_secret]
)
class SieveImporter:
    @modal.fastapi_endpoint(method="POST", requires_proxy_auth=True)
    def import_video(self, request: SieveImportRequest) -> SieveImportResponse:

        video_uuid = str(uuid.uuid4())
        s3_key = f"sieve/{video_uuid}.mp4"
        s3_path = f"/s3-mount/{s3_key}"
        os.makedirs(os.path.dirname(s3_path), exist_ok=True)

        try:
            cmd = [
                "curl",
                "-L",
                "--fail",
                "--proto-default", "https",
                "--retry", "3",
                "--retry-delay", "2",
                "-o", s3_path,
                request.video_url
            ]
            subprocess.run(cmd, check=True)
        except Exception:
            if os.path.exists(s3_path):
                os.remove(s3_path)
            raise

        return SieveImportResponse(s3_key=s3_key)


@app.local_entrypoint()
def main():
    import requests

    test_url = "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/voices/bezos.wav"

    server = SieveImporter()
    endpoint_url = server.import_video.get_web_url()

    request = SieveImportRequest(
        video_url=test_url
    )

    payload = request.model_dump()

    headers = {
        "Modal-Key": "wk-l1RbNAi2YXIzw4AqvNjHU5",
        "Modal-Secret": "ws-3UxlJXh3IzFyB6gJLikGya"
    }

    response = requests.post(endpoint_url, json=payload, headers=headers)
    response.raise_for_status()

    result = SieveImportResponse(**response.json())

    print(result.s3_key)
