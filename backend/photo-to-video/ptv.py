import glob
import os
import shutil
import subprocess
import tempfile

import uuid

import modal
from pydantic import BaseModel

app = modal.App("hallo3-portrait-avatar")

volume = modal.Volume.from_name("hallo3-cache", create_if_missing=True)

volumes = {"/models": volume}


def download_hallo3_models():
    from huggingface_hub import snapshot_download
    snapshot_download(
        "fudan-generative-ai/hallo3",
        local_dir="/models/pretrained_models",
        ignore_patterns=[]
    )


image = (
    modal.Image
    .from_registry("nvidia/cuda:12.1.1-devel-ubuntu20.04", add_python="3.10")
    .env({"DEBIAN_FRONTEND": "noninteractive"})
    .apt_install("git", "ffmpeg", "clang", "libaio-dev")
    .pip_install_from_requirements("photo-to-video/requirements.txt")
    .run_commands("git clone https://github.com/fudan-generative-vision/hallo3 /hallo3")
    .run_commands("ln -s /models/pretrained_models /hallo3/pretrained_models")
    .run_function(download_hallo3_models, volumes=volumes)
)

s3_secret = modal.Secret.from_name("hey-gen-secret")


class PortraitAvatarRequest(BaseModel):
    transcript: str
    photo_s3_key: str
    audio_s3_key: str


class PortraitAvatarResponse(BaseModel):
    video_s3_key: str


@app.cls(
    image=image,
    gpu="A100-80GB",
    volumes={
        **volumes,
        "/s3-mount": modal.CloudBucketMount("hey-gen-clone", secret=s3_secret)
    },
    timeout=2700,
    secrets=[s3_secret]
)
class PortraitAvatarServer:
    @modal.fastapi_endpoint(method="POST", requires_proxy_auth=True)
    def generate_video(self, request: PortraitAvatarRequest) -> PortraitAvatarResponse:
        print(
            f"Received request to generate portrait avatar: {request.transcript} {request.photo_s3_key} {request.audio_s3_key}")

        temp_dir = tempfile.mkdtemp()

        try:
            photo_path = f"/s3-mount/{request.photo_s3_key}"
            audio_path = f"/s3-mount/{request.audio_s3_key}"

            if not os.path.exists(photo_path):
                raise FileNotFoundError(f"Photo not found at {photo_path}")
            if not os.path.exists(audio_path):
                raise FileNotFoundError(f"Photo not found at {audio_path}")

            input_txt_path = os.path.join(temp_dir, "input.txt")
            with open(input_txt_path, "w") as f:
                f.write(f"{request.transcript}@@{photo_path}@@{audio_path}\n")

            print("Generating video with Hallo3")
            output_dir = os.path.join(temp_dir, "output")
            os.makedirs(output_dir, exist_ok=True)

            command = [
                "bash",
                "/hallo3/scripts/inference_long_batch.sh",
                input_txt_path,
                output_dir
            ]

            subprocess.run(command, check=True, cwd="/hallo3")
            print("Processing finished")

            generated_video_file = None
            for fpath in glob.glob(os.path.join(output_dir, "**", "*.mp4"), recursive=True):
                generated_video_file = fpath
                break
            if not generated_video_file:
                raise RuntimeError("Hallo3 did not produce a video file.")

            print("Merging the video and audio with ffmpeg.")
            final_video_path = os.path.join(temp_dir, "final_video.mp4")
            ffmpeg_command = [
                "ffmpeg",
                "-i", generated_video_file,
                "-i", audio_path,
                "-c:v", "copy",
                "-c:a", "aac",
                "-shortest",
                final_video_path
            ]

            subprocess.run(ffmpeg_command, check=True)
            print(f"Final video created at {final_video_path}")

            video_uuid = str(uuid.uuid4())
            s3_key = f"ptv/{video_uuid}.mp4"
            s3_path = f"/s3-mount/{s3_key}"
            os.makedirs(os.path.dirname(s3_path), exist_ok=True)
            shutil.copy(final_video_path, s3_path)
            print(f"Saved video to S3: {s3_key}")

            return PortraitAvatarResponse(video_s3_key=s3_key)
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


@app.local_entrypoint()
def main():
    import requests

    server = PortraitAvatarServer()
    endpoint_url = server.generate_video.get_web_url()

    request = PortraitAvatarRequest(
        transcript="",
        photo_s3_key="samples/photos/0008.jpg",
        audio_s3_key="samples/voices/myvoice.wav"
    )

    payload = request.model_dump()

    headers = {
        "Modal-Key": "wk-l1RbNAi2YXIzw4AqvNjHU5",
        "Modal-Secret": "ws-3UxlJXh3IzFyB6gJLikGya"
    }

    response = requests.post(endpoint_url, json=payload, headers=headers)
    response.raise_for_status()

    result = PortraitAvatarResponse(**response.json())

    print(result.video_s3_key)
