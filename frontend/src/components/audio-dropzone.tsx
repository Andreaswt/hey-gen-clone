import { UploadCloud } from "lucide-react";
import { useRef, type ChangeEvent } from "react";

export default function AudioDropzone({
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
    if (file?.type.startsWith("audio/")) {
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
      <UploadCloud className="h-10 w-10 text-gray-400" />
      <span className="font-semibold">Upload a file</span>
      <span className="text-sm text-gray-500">
        Supported formats: .wav (max 15s)
      </span>
      <input
        ref={fileInputRef}
        type="file"
        accept="audio/wav"
        className="hidden"
        onChange={handleFileChange}
      />
    </div>
  );
}
