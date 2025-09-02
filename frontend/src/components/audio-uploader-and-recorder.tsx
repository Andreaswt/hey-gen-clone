import { useEffect, useRef, useState } from "react";
import AudioDropzone from "./audio-dropzone";
import { toast } from "sonner";
import { getAudioDuration } from "~/utils/media";
import { Button } from "./ui/button";

export function AudioUploaderAndRecorder({
  onAudioReady,
}: {
  onAudioReady: (audioBlob: Blob) => void;
}) {
  const [audioUrl, setAudioUrl] = useState<string | null>(null);
  const [audioBlob, setAudioBlob] = useState<Blob | null>(null);
  const [isRecording, setIsRecording] = useState(false);
  const [recordTime, setRecordTime] = useState(0);
  const audioChunks = useRef<Blob[]>([]);
  const mediaRecorder = useRef<MediaRecorder | null>(null);

  useEffect(() => {
    let timer: NodeJS.Timeout;
    if (isRecording) {
      timer = setInterval(() => {
        setRecordTime((t) => {
          if (t + 1 >= 15) {
            void stopRecording();
            return 15;
          }
          return t + 1;
        });
      }, 1000);
    } else {
      setRecordTime(0);
    }
    return () => clearInterval(timer);
  }, [isRecording]);

  const handleFileSelect = async (file: File) => {
    try {
      const duration = await getAudioDuration(file);
      if (duration > 15) {
        toast.error("Audio must be 15 seconds or less.");
        return;
      }
      setAudioUrl(URL.createObjectURL(file));
      setAudioBlob(file);
    } catch {
      toast.error("Could not read the audio file.");
    }
  };

  const startRecording = async () => {
    setAudioUrl(null);
    audioChunks.current = [];
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const recorder = new MediaRecorder(stream);
      mediaRecorder.current = recorder;
      recorder.ondataavailable = (event) => {
        if (event.data.size > 0) audioChunks.current.push(event.data);
      };
      recorder.onstop = () => {
        const blob = new Blob(audioChunks.current, { type: "audio/webm" });
        const url = URL.createObjectURL(blob);
        setAudioUrl(url);
        setAudioBlob(blob);
      };
      recorder.start();
      setIsRecording(true);
    } catch {
      toast.error(
        "Could not start recording. Please check microphone permissions.",
      );
    }
  };

  const stopRecording = async () => {
    if (mediaRecorder.current?.state === "recording") {
      mediaRecorder.current.stop();
    }
    setIsRecording(false);
  };

  const handleUseAudio = () => {
    if (audioBlob) {
      onAudioReady(audioBlob);
    }
  };

  return (
    <div className="flex flex-col gap-4">
      {!audioUrl ? (
        <>
          <AudioDropzone onFileSelect={handleFileSelect} />
          <div className="relative flex items-center">
            <div className="flex-grow border-t border-gray-200"></div>
            <span className="mx-4 flex-shrink text-sm text-gray-500">OR</span>
            <div className="flex-grow border-t border-gray-200"></div>
          </div>
          {isRecording ? (
            <div className="flex w-full flex-col items-center gap-2">
              <span className="text-xs text-gray-700">
                Recording... 00:{String(recordTime).padStart(2, "0")} / 00:15
              </span>
              <Button
                onClick={stopRecording}
                variant="destructive"
                className="w-full cursor-pointer"
              >
                Record audio
              </Button>
            </div>
          ) : (
            <Button
              className="cursor-pointer"
              onClick={startRecording}
              variant="secondary"
            >
              Record audio
            </Button>
          )}
        </>
      ) : (
        <div className="flex w-full flex-col items-center gap-2">
          <audio
            className="mt-2 w-full"
            key={audioUrl}
            controls
            src={audioUrl}
          />
          <Button
            variant="default"
            className="flex w-full cursor-pointer items-center justify-center"
            onClick={handleUseAudio}
          >
            Use this audio
          </Button>
          <Button
            variant="ghost"
            className="w-full cursor-pointer"
            onClick={() => setAudioUrl(null)}
          >
            Use a different audio
          </Button>
        </div>
      )}
    </div>
  );
}
