import { Pause, Play } from "lucide-react";
import { AudioUploaderAndRecorder } from "./audio-uploader-and-recorder";
import { Button } from "./ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "./ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./ui/tabs";
import { useAudioPlayer } from "~/hooks/useAudioPlayer";

export interface Voice {
  id: string;
  name: string;
  tags: string[];
  accent: string;
  flag: string;
  audioSrc: string;
  s3Key: string;
}

const heyGenLibraryVoice: Voice[] = [
  {
    id: "v1",
    name: "Jeff",
    tags: ["Deep", "American", "Assertive"],
    accent: "American",
    flag: "🇺🇸",
    audioSrc:
      "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/voices/bezos.wav",
    s3Key: "samples/voices/bezos.wav",
  },
  {
    id: "v2",
    name: "Kamala",
    tags: ["Articulate", "American"],
    accent: "American",
    flag: "🇺🇸",
    audioSrc:
      "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/voices/harris.wav",
    s3Key: "samples/voices/harris.wav",
  },
  {
    id: "v3",
    name: "Mark",
    tags: ["Direct", "American"],
    accent: "American",
    flag: "🇺🇸",
    audioSrc:
      "https://public-hey-gen-clone.s3.us-east-1.amazonaws.com/samples/voices/zuck.wav",
    s3Key: "samples/voices/zuck.wav",
  },
];

function VoiceCard({
  voice,
  onSelect,
  onPlayToggle,
  isPlaying,
}: {
  voice: Voice;
  onSelect: (voice: Voice) => void;
  onPlayToggle: (voice: Voice) => void;
  isPlaying: boolean;
}) {
  const handlePlayClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onPlayToggle(voice);
  };

  return (
    <div
      onClick={() => onSelect(voice)}
      className="flex w-52 cursor-pointer flex-col gap-2 rounded-lg border bg-white p-4 transition-all hover:shadow-md"
    >
      <div className="flex items-center justify-between">
        <span className="rounded bg-green-100 px-2 py-0.5 text-xs text-green-700">
          Public
        </span>
        <Button variant="ghost" size="icon" onClick={handlePlayClick}>
          {isPlaying ? (
            <Pause className="h-5 w-5 text-purple-600" />
          ) : (
            <Play className="h-5 w-5 text-purple-600" />
          )}
        </Button>
      </div>
      <div className="flex flex-col">
        <span className="font-semibold">
          {voice.flag} {voice.name}
        </span>
        <span className="text-xs text-gray-500">{voice.tags.join(", ")}</span>
      </div>
    </div>
  );
}

export default function ChooseVoiceModal({
  open,
  onOpenChange,
  onVoiceSelected,
  onAudioUploaded,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onVoiceSelected: (voice: Voice) => void;
  onAudioUploaded: (file: File) => void;
}) {
  const { playingSrc, togglePlay } = useAudioPlayer();
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle className="text-xl font-bold">Choose Voice</DialogTitle>
        </DialogHeader>
        <Tabs defaultValue="heygen-library">
          <TabsList>
            <TabsTrigger value="my-voices">My Voices</TabsTrigger>
            <TabsTrigger value="heygen-library">HeyGen Library</TabsTrigger>
          </TabsList>
          <TabsContent value="my-voices" className="p-4">
            <AudioUploaderAndRecorder
              onAudioReady={function (blob: Blob): void {
                const file = new File([blob], "custom_voice.wav", {
                  type: blob.type,
                });

                onAudioUploaded(file);
                onOpenChange(false);
              }}
            />
          </TabsContent>
          <TabsContent
            value="heygen-library"
            className="flex max-h-[60vh] flex-wrap gap-4 overflow-y-auto p-4"
          >
            {heyGenLibraryVoice.map((voice) => (
              <VoiceCard
                key={voice.id}
                voice={voice}
                onSelect={(voice: Voice) => {
                  onVoiceSelected(voice);
                }}
                onPlayToggle={(voice: Voice) => togglePlay(voice.audioSrc)}
                isPlaying={playingSrc === voice.audioSrc}
              />
            ))}
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
