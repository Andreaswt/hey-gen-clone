import { UploadCloud, Video } from "lucide-react";
import { useRef, type ChangeEvent } from "react";

export default function VideoDropzone({
  onFileSelect,
}: {
  onFileSelect: (file: File) => void;
}) {
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      onFileSelect(file);
    }
  };

  const handleDrop = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    const file = e.dataTransfer.files?.[0];
    if (file?.type.startsWith("video/")) {
      onFileSelect(file);
    }
  };

  const handleDragOver = (e: React.DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
  };

  return (
    <div
      onDrop={handleDrop}
      onDragOver={handleDragOver}
      onClick={() => fileInputRef.current?.click()}
      className="flex h-48 cursor-pointer flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed"
    >
      <Video className="h-10 w-10 text-gray-400" />
      <span className="font-semibold">Upload a video</span>
      <span className="text-sm text-gray-500">Browse local files</span>
      <input
        ref={fileInputRef}
        type="file"
        accept="audio/mp4"
        className="hidden"
        onChange={handleFileChange}
      />
    </div>
  );
}
