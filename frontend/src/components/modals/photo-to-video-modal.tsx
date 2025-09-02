import {
  AudioWaveform,
  Loader2,
  Pause,
  Play,
  Ratio,
  Trash2,
  UploadCloud,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "../ui/dialog";
import { Input } from "../ui/input";
import { useRef, useState } from "react";
import { Button } from "../ui/button";
import { Textarea } from "../ui/textarea";
import AudioUploadModal from "../audio-upload-modal";
import ChooseVoiceModal, { type Voice } from "../choose-voice-modal";
import { useAudioPlayer } from "~/hooks/useAudioPlayer";
import Cropper, { type Area } from "react-easy-crop";
import { Switch } from "../ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../ui/select";
import getCroppedImg from "~/utils/cropImage";
import { getPresignedUrl, photoToVideo } from "~/actions/generation";
import { toast } from "sonner";

const samplePhotos = [
  {
    s3Key: "samples/photos/0008.jpg",
    url: "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/photos/0008.jpg",
  },
  {
    s3Key: "samples/photos/0009.jpg",
    url: "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/photos/0009.jpg",
  },
  {
    s3Key: "samples/photos/0022.jpg",
    url: "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/photos/0022.jpg",
  },
];

export function PhotoToVideoModal({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [selectedPhotoUrl, setSelectedPhotoUrl] = useState<string | null>(null);
  const [selectedPhotoFile, setSelectedPhotoFile] = useState<File | null>(null);
  const [script, setScript] = useState("");
  const [audioModalOpen, setAudioModalOpen] = useState(false);

  const audioRef = useRef<HTMLAudioElement>(null);
  const [selectedAudioUrl, setSelectedAudioUrl] = useState<string | null>(null);
  const [selectedAudioName, setSelectedAudioName] =
    useState<string>("new_recording.wav");
  const [voiceModalOpen, setVoiceModalOpen] = useState(false);
  const [selectedVoice, setSelectedVoice] = useState<Voice | null>(null);
  const [customVoiceFile, setCustomVoiceFile] = useState<File | null>(null);

  const [experimentalModel, setExperimentalModel] = useState(false);
  const [crop, setCrop] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [aspectRatio, setAspectRatio] = useState(1 / 1);
  const [croppedAreaPixels, setCroppedAreaPixels] = useState<Area | null>(null);

  const [enhancement, setEnhancement] = useState(true);
  const [expressiveness, setExpressiveness] = useState(1);
  const [resolution, setResolution] = useState("512");

  const [loading, setLoading] = useState(false);

  const { playingSrc, togglePlay } = useAudioPlayer();

  const handlePhotoFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setSelectedPhotoUrl(URL.createObjectURL(file));
      setSelectedPhotoFile(file);
    }
  };

  const handleGenerateVideo = async () => {
    setLoading(true);

    let photoS3Key: string | null = null;
    let audioS3Key: string | null = null;
    let voiceS3Key: string | null = null;

    // 1. Upload the photo
    if (selectedPhotoUrl) {
      let fileToUpload: File | null = null;

      if (!experimentalModel && croppedAreaPixels) {
        const croppedImage = await getCroppedImg(
          selectedPhotoUrl,
          croppedAreaPixels,
        );
        if (croppedImage) {
          fileToUpload = croppedImage.file;
        }
      } else if (selectedPhotoFile) {
        fileToUpload = selectedPhotoFile;
      }

      if (fileToUpload) {
        // Get presigned URL
        const { url, key } = await getPresignedUrl(
          fileToUpload.name,
          fileToUpload.type,
          "ptvPhoto",
        );

        // Make fetch PUT request to URL to upload to S3
        await fetch(url, {
          method: "PUT",
          headers: {
            "Content-Type": fileToUpload.type,
          },
          body: fileToUpload,
        });

        photoS3Key = key;
      } else {
        const sample = samplePhotos.find((p) => p.url === selectedPhotoUrl);
        photoS3Key = sample ? sample.s3Key : null;
      }
    }

    // 2. Upload the driving audio
    if (selectedAudioUrl && selectedAudioName) {
      const response = await fetch(selectedAudioUrl);
      const audioBlob = await response.blob();
      const audioFile = new File([audioBlob], selectedAudioName, {
        type: audioBlob.type,
      });

      const { url, key } = await getPresignedUrl(
        audioFile.name,
        audioFile.type,
        "ptvAudio",
      );

      await fetch(url, {
        method: "PUT",
        headers: {
          "Content-Type": audioFile.type,
        },
        body: audioFile,
      });

      audioS3Key = key;
    }

    // 3. Upload the custom voice
    if (customVoiceFile) {
      const { url, key } = await getPresignedUrl(
        customVoiceFile.name,
        customVoiceFile.type,
        "ttsVoiceClone",
      );

      await fetch(url, {
        method: "PUT",
        headers: {
          "Content-Type": customVoiceFile.type,
        },
        body: customVoiceFile,
      });

      voiceS3Key = key;
    } else if (selectedVoice) {
      voiceS3Key = selectedVoice.s3Key;
    }

    // 4. Call server action
    await photoToVideo({
      photoS3Key: photoS3Key!,
      script: script,
      audioS3Key,
      voiceS3Key,
      expressiveness,
      enhancement,
      experimentalModel,
      resolution,
    });

    toast.success(
      "Photo to video job queued! You will be able to see the status on the dashboard.",
    );

    setLoading(false);
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="h-fit max-h-[95%] w-full min-w-[95%] overflow-y-auto lg:w-full lg:max-w-5xl lg:min-w-fit">
        <div className="flex flex-col gap-0">
          <DialogHeader>
            <DialogTitle className="text-2xl font-semibold">
              Realistic talking video with{" "}
              <span className="text-purple-600">AI Portrait Avatars</span>
            </DialogTitle>
            <DialogDescription className="mt-2 text-base">
              Turn a single photo and script into a high-quality avatar video
              using an AI portrait model.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-8 p-8 lg:flex-row">
            {/* Left: Upload photo */}
            <div className="flex w-full flex-col gap-4 lg:w-[340px]">
              {selectedPhotoUrl && !experimentalModel ? (
                <div className="relative h-64 w-64">
                  <Cropper
                    classes={{ containerClassName: "rounded-md" }}
                    image={selectedPhotoUrl}
                    crop={crop}
                    zoom={zoom}
                    aspect={aspectRatio}
                    onCropChange={setCrop}
                    onZoomChange={setZoom}
                    onCropComplete={(_, pixels) => {
                      setCroppedAreaPixels(pixels);
                    }}
                  />
                  <Button
                    onClick={() => {
                      if (aspectRatio === 1 / 1) {
                        setAspectRatio(3 / 2);
                      } else {
                        setAspectRatio(1 / 1);
                      }
                    }}
                    className="absolute top-2 right-2 cursor-pointer rounded-full bg-white p-2 shadow hover:bg-gray-200"
                  >
                    <Ratio className="h-5 w-5 text-gray-600" />
                  </Button>
                  <Button
                    onClick={() => {
                      setSelectedPhotoUrl(null);
                      setSelectedPhotoFile(null);
                    }}
                    className="absolute top-14 right-2 cursor-pointer rounded-full bg-white p-2 shadow hover:bg-gray-200"
                  >
                    <Trash2 className="h-5 w-5 text-gray-600" />
                  </Button>
                </div>
              ) : selectedPhotoUrl ? (
                <div className="flex flex-col items-center justify-between gap-4">
                  <div className="relative">
                    <img
                      src={selectedPhotoUrl}
                      crossOrigin="anonymous"
                      className="max-h-[340px] max-w-full rounded-xl border object-contain md:max-w-[340px]"
                    />
                    <Button
                      variant="ghost"
                      onClick={() => {
                        setSelectedPhotoFile(null);
                        setSelectedPhotoUrl(null);
                      }}
                      className="absolute top-2 right-2 cursor-pointer rounded-full bg-white p-2 shadow"
                    >
                      <Trash2 className="h-5 w-5 text-gray-600" />
                    </Button>
                  </div>
                </div>
              ) : (
                <div className="flex h-full flex-col items-center rounded-xl border-2 border-dashed border-gray-200 bg-gray-50 px-4 pt-8 pb-4">
                  <div className="flex flex-grow flex-col items-center justify-center">
                    <UploadCloud className="mb-2 h-10 w-10 text-gray-400" />
                    <label className="cursor-pointer font-medium underline">
                      Upload photo
                      <Input
                        onChange={handlePhotoFileChange}
                        type="file"
                        accept="image/*"
                        className="hidden"
                      />
                    </label>
                    <div className="mt-2 text-xs text-gray-500">
                      For best results, choose a photo that&apos;s at least 720p
                    </div>
                  </div>
                  <div className="mt-6 w-full">
                    <div className="text-xs text-gray-500">
                      Try a sample photo
                    </div>
                    <div className="mt-1 flex gap-2">
                      {samplePhotos.map((item) => (
                        <img
                          key={item.s3Key}
                          crossOrigin="anonymous"
                          className="h-14 w-14 cursor-pointer rounded border object-cover"
                          src={item.url}
                          onClick={() => {
                            setSelectedPhotoUrl(item.url);
                            setSelectedPhotoFile(null);
                          }}
                        />
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Right: script and options */}
            <div className="flex flex-1 flex-col gap-4">
              <div>
                <div className="relative">
                  {selectedAudioUrl ? (
                    <div className="flex items-center gap-3 rounded border bg-gray-50 p-3">
                      <button
                        className="mr-2 cursor-pointer"
                        onClick={async () => {
                          const audio = audioRef.current;
                          if (!audio) return;
                          if (audio.paused) {
                            await audio.play();
                          } else {
                            audio.pause();
                          }
                        }}
                      >
                        <Play className="h-5 w-5 text-gray-600" />
                      </button>
                      <div className="flex flex-1 flex-col">
                        <span className="text-sm font-medium">
                          {selectedAudioName}
                        </span>
                      </div>
                      <button
                        className="ml-2 cursor-pointer"
                        onClick={() => {
                          setSelectedAudioUrl(null);
                        }}
                      >
                        <Trash2 className="h-5 w-5 text-gray-600" />
                      </button>
                      <audio
                        id="audio-preview"
                        ref={audioRef}
                        src={selectedAudioUrl}
                        style={{ display: "none" }}
                      />
                    </div>
                  ) : (
                    <>
                      {" "}
                      <Textarea
                        value={script}
                        onChange={(e) => setScript(e.target.value)}
                        rows={10}
                        maxLength={210}
                        placeholder="Type your script here or"
                        className="min-h-32 break-all"
                      />
                      <Button
                        variant="link"
                        size="sm"
                        onClick={() => setAudioModalOpen(true)}
                        className={`${script ? "hidden" : ""} absolute top-7 left-0.5 cursor-pointer text-base underline md:text-sm lg:top-[3px] lg:left-[156px]`}
                      >
                        upload or record audio
                      </Button>
                    </>
                  )}

                  {/* Container at the bottom of the textarea */}
                  <div className="absolute bottom-2 flex w-full flex-col gap-2 px-3">
                    <div className="mt-2 flex items-center gap-2">
                      {!selectedAudioUrl &&
                        (customVoiceFile ? (
                          <div className="flex items-center gap-2">
                            <button
                              className="flex cursor-pointer items-center gap-1 text-xs font-medium text-gray-600 hover:text-purple-600"
                              onClick={() => setVoiceModalOpen(true)}
                            >
                              <AudioWaveform className="h-4 w-4" />
                              <span>{customVoiceFile.name}</span>
                            </button>
                          </div>
                        ) : selectedVoice ? (
                          <div className="flex items-center gap-2">
                            <button
                              onClick={() => setVoiceModalOpen(true)}
                              className="flex cursor-pointer items-center gap-1 text-xs font-medium text-gray-600 hover:text-purple-600"
                            >
                              <AudioWaveform className="h-4 w-4" />
                              <span>{selectedVoice.name}</span>
                            </button>
                            <button
                              className="cursor-pointer text-purple-500"
                              onClick={() => {
                                if (selectedVoice) {
                                  togglePlay(selectedVoice.audioSrc);
                                }
                              }}
                            >
                              {playingSrc === selectedVoice.audioSrc ? (
                                <Pause className="h-5 w-5 rounded-full bg-purple-100 p-1" />
                              ) : (
                                <Play className="h-5 w-5 rounded-full bg-purple-100 p-1" />
                              )}
                            </button>
                          </div>
                        ) : (
                          <Button
                            variant="link"
                            className="h-0 px-0 text-xs text-gray-500"
                            onClick={() => setVoiceModalOpen(true)}
                          >
                            Select voice
                          </Button>
                        ))}
                      {!selectedAudioUrl && (
                        <span className="ml-auto text-xs text-gray-400">
                          {script.length} / 210 (15 seconds max)
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex gap-6">
                <div className="flex items-center gap-2">
                  <Switch
                    checked={experimentalModel}
                    onCheckedChange={setExperimentalModel}
                  />
                  <span className="text-xs text-gray-700">
                    Experimental model
                  </span>
                </div>
                {experimentalModel && (
                  <div className="flex items-center gap-2">
                    <Switch
                      checked={enhancement}
                      onCheckedChange={setEnhancement}
                    />
                    <span className="text-xs text-gray-700">Enhancement</span>
                  </div>
                )}
              </div>

              <div className="mt-2 flex flex-wrap gap-6">
                {experimentalModel && (
                  <div>
                    <label className="text-sm font-medium">
                      Expressiveness{" "}
                      <span className="ml-1 rounded bg-gray-100 px-1 text-xs">
                        0-1
                      </span>
                    </label>
                    <Input
                      type="number"
                      min={0}
                      max={1}
                      step={0.01}
                      value={expressiveness}
                      onChange={(e) => {
                        setExpressiveness(Number(e.target.value));
                      }}
                      className="mt-2 w-24"
                    />
                  </div>
                )}

                <div className="flex w-full items-center justify-end">
                  {experimentalModel && (
                    <Select
                      value={resolution}
                      onValueChange={(value) => setResolution(value)}
                    >
                      <SelectTrigger className="w-[100px] rounded border px-2 py-1 text-xs">
                        <SelectValue placeholder="512p" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="512">512p</SelectItem>
                        <SelectItem value="640">640p</SelectItem>
                      </SelectContent>
                    </Select>
                  )}
                  <Button
                    onClick={handleGenerateVideo}
                    className="ml-2 bg-purple-600 px-4 py-2 text-sm text-white"
                    disabled={
                      loading ||
                      !(
                        (selectedPhotoFile ?? selectedPhotoUrl) &&
                        (script.trim().length > 0 || selectedAudioUrl)
                      )
                    }
                  >
                    {loading ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    ) : null}
                    Generate video
                  </Button>
                </div>
              </div>
            </div>
          </div>
        </div>
        <AudioUploadModal
          open={audioModalOpen}
          onOpenChange={setAudioModalOpen}
          onAudioRecorded={(audioBlob: Blob) => {
            const url = URL.createObjectURL(audioBlob);
            setSelectedAudioUrl(url);

            let name =
              "new_recording_" +
              new Date().toLocaleString().replace(/[\s:/]/g, "_") +
              ".wav";
            if ((audioBlob as File).name) {
              name = (audioBlob as File).name;
            }
            setSelectedAudioName(name);
            setAudioModalOpen(false);
          }}
        />
        <ChooseVoiceModal
          open={voiceModalOpen}
          onOpenChange={setVoiceModalOpen}
          onVoiceSelected={(voice) => {
            setSelectedVoice(voice);
            setCustomVoiceFile(null);
            setVoiceModalOpen(false);
          }}
          onAudioUploaded={(file) => {
            setSelectedVoice(null);
            setCustomVoiceFile(file);
            setVoiceModalOpen(false);
          }}
        />
      </DialogContent>
    </Dialog>
  );
}
