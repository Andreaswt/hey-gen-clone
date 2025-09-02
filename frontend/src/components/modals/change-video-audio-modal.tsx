import {
  ArrowLeft,
  Loader2,
  Pause,
  Play,
  Trash2,
  UploadCloud,
} from "lucide-react";
import { useEffect, useState } from "react";
import { toast } from "sonner";
import { getVideoDuration } from "~/utils/media";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "../ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../ui/select";
import VideoDropzone from "../video-dropzone";
import {
  changeVideoAudio,
  getPresignedUrl,
  translateVideo,
} from "~/actions/generation";
import { useAudioPlayer } from "~/hooks/useAudioPlayer";
import AudioUploadModal from "../audio-upload-modal";

export function ChangeVideoAudioModal({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const [step, setStep] = useState(1);
  const [videoFile, setVideoFile] = useState<File | null>(null);
  const [videoUrl, setVideoUrl] = useState<string | null>(null);
  const [audioFile, setAudioFile] = useState<File | null>(null);
  const [audioUrl, setAudioUrl] = useState<string | null>(null);
  const [audioName, setAudioName] = useState<string | null>(null);
  const [audioModalOpen, setAudioModalOpen] = useState(false);
  const { playingSrc, togglePlay } = useAudioPlayer();

  const [loading, setLoading] = useState(false);

  const handleFileSelect = async (file: File) => {
    try {
      const fileUrl = URL.createObjectURL(file);
      const duration = await getVideoDuration(fileUrl);
      if (duration > 180) {
        toast.error("Video must be 3 minutes or less.");
        URL.revokeObjectURL(fileUrl);
        return;
      }
      setVideoFile(file);
      setVideoUrl(fileUrl);
      setStep(2);
    } catch {
      toast.error("Could not read the video file.");
    }
  };

  const handleGenerate = async () => {
    if (!videoFile || !audioFile) {
      toast.error("Please select a video and an audio file.");
      return;
    }

    setLoading(true);

    const [videoUpload, audioUpload] = await Promise.all([
      getPresignedUrl(videoFile.name, videoFile.type, "cvaVideo"),
      getPresignedUrl(audioFile.name, audioFile.type, "cvaAudio"),
    ]);

    await Promise.all([
      fetch(videoUpload.url, {
        method: "PUT",
        headers: { "Content-Type": videoFile.type },
        body: videoFile,
      }),
      fetch(audioUpload.url, {
        method: "PUT",
        headers: { "Content-Type": audioFile.type },
        body: audioFile,
      }),
    ]);

    await changeVideoAudio({
      sourceVideoS3Key: videoUpload.key,
      newAudioS3Key: audioUpload.key,
      videoName: videoFile.name,
    });

    toast.success(
      "Translation job queued! You will be notified upon completion.",
    );
    onOpenChange(false);
    setLoading(false);
  };

  useEffect(() => {
    if (!open) {
      setStep(1);
      setVideoFile(null);
      if (videoUrl) {
        URL.revokeObjectURL(videoUrl);
      }
      setVideoUrl(null);
      if (audioUrl) {
        URL.revokeObjectURL(audioUrl);
      }
      setAudioUrl(null);
      setAudioFile(null);
      setVideoUrl(null);
      setLoading(false);
    }
  }, [open, videoUrl, audioUrl]);

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="h-fit max-h-[95%] w-full min-w-[95%] overflow-y-auto lg:w-full lg:max-w-5xl lg:min-w-fit">
          <DialogHeader>
            <DialogTitle className="text-2xl font-semibold">
              Change Video Audio
            </DialogTitle>
            <DialogDescription className="mt-2 text-base">
              Replace the audio of your video with a new track. The video can be
              up to <span className="text-purple-600">3 minutes</span> in
              length.
            </DialogDescription>
          </DialogHeader>

          {step === 1 && <VideoDropzone onFileSelect={handleFileSelect} />}

          {step === 2 && (
            <div className="grid grid-cols-1 gap-8 md:grid-cols-2">
              <div className="flex flex-col gap-4">
                <h3 className="font-semibold">Source Video</h3>
                <div className="relative">
                  {videoUrl && (
                    <video
                      src={videoUrl}
                      controls
                      className="w-full rounded-lg"
                    />
                  )}
                  <Button
                    onClick={() => {
                      setStep(1);
                      setVideoFile(null);
                    }}
                    className="absolute top-2 right-2 cursor-pointer rounded-full bg-black/50 text-white hover:bg-black/70"
                    variant="ghost"
                    size="icon"
                  >
                    <Trash2 className="h-5 w-5 text-white" />
                  </Button>
                </div>
              </div>

              <div className="flex flex-col gap-4">
                <h3 className="font-semibold">New Audio</h3>
                {audioUrl && audioName ? (
                  <div className="flex flex-col justify-center rounded-xl border bg-gray-50 p-4">
                    <div className="flex items-center gap-3">
                      <button
                        className="mr-2"
                        onClick={() => togglePlay(audioUrl)}
                      >
                        {playingSrc === audioUrl ? (
                          <Pause className="h-5 w-5 text-gray-600" />
                        ) : (
                          <Play className="h-5 w-5 text-gray-600" />
                        )}
                      </button>
                      <div className="flex flex-1 flex-col">
                        <span className="text-sm font-medium">{audioName}</span>
                      </div>
                      <button
                        onClick={() => {
                          setAudioUrl(null);
                          setAudioName(null);
                          setAudioFile(null);
                        }}
                      >
                        <Trash2 className="h-5 w-5 text-gray-600" />
                      </button>
                    </div>
                  </div>
                ) : (
                  <div
                    onClick={() => setAudioModalOpen(true)}
                    className="flex h-48 cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed"
                  >
                    <UploadCloud className="h-10 w-10 text-gray-400" />
                    <span className="font-semibold">
                      Upload or Record Audio
                    </span>
                    <span className="text-sm text-gray-500">
                      Click here to select an audio file or record a new one.
                    </span>
                  </div>
                )}
              </div>
            </div>
          )}

          <div className="mt-4 flex justify-between">
            <Button
              variant="outline"
              onClick={() => (step === 1 ? onOpenChange(false) : setStep(1))}
            >
              {step === 2 && <ArrowLeft className="mr-2 h-4 w-4" />}
              Back
            </Button>
            {step === 2 && (
              <Button
                onClick={handleGenerate}
                disabled={loading || !audioUrl}
                className="cursor-pointer bg-purple-600 text-white hover:bg-purple-700"
              >
                {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                Generate
              </Button>
            )}
          </div>
        </DialogContent>
      </Dialog>
      <AudioUploadModal
        open={audioModalOpen}
        onOpenChange={setAudioModalOpen}
        onAudioRecorded={(audioBlob: Blob) => {
          const url = URL.createObjectURL(audioBlob);
          setAudioUrl(url);

          const file =
            audioBlob instanceof File
              ? audioBlob
              : new File(
                  [audioBlob],
                  "new_recording_" +
                    new Date().toLocaleString().replace(/[\s:/]/g, "_") +
                    ".wav",
                  { type: audioBlob.type },
                );

          setAudioName(file.name);
          setAudioFile(file);
          setAudioModalOpen(false);
        }}
      />
    </>
  );
}
