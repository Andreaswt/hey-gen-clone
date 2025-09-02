import { AudioUploaderAndRecorder } from "./audio-uploader-and-recorder";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";

export default function AudioUploadModal({
  open,
  onOpenChange,
  onAudioRecorded,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAudioRecorded: (audioBlob: Blob) => void;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Upload or record audio</DialogTitle>
        </DialogHeader>
        <AudioUploaderAndRecorder
          onAudioReady={(blob) => {
            onAudioRecorded(blob);
            onOpenChange(false);
          }}
        />
      </DialogContent>
    </Dialog>
  );
}
