"use client";

import {
  Download,
  Image,
  Info,
  Languages,
  Play,
  RefreshCw,
  Video,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { PhotoToVideoModal } from "./modals/photo-to-video-modal";
import { Button } from "./ui/button";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { getVideoDuration } from "~/utils/media";
import { updateCreationName } from "~/actions/creations";
import { VideoTranslationModal } from "./modals/video-translation-modal";
import { ChangeVideoAudioModal } from "./modals/change-video-audio-modal";
import { toast } from "sonner";

const features = [
  {
    label: "Photo to Video with Portrait Avatar",
    icon: Image,
    color: "bg-blue-50 text-blue-500",
    description: "Turn photo and script into talking video.",
  },
  {
    label: "Translate Video",
    icon: Languages,
    color: "bg-orange-50 text-orange-500",
    description: "Translate with original voice and lip sync.",
  },
  {
    label: "Change Video Audio",
    icon: Video,
    color: "bg-purple-50 text-purple-500",
    description: "Replace the audio track of your video.",
  },
];

type Creation = {
  id: string;
  title: string;
  date: string;
  type: string;
  videoUrl: string | null;
  status: string;
};

export function ClientHome({
  recentCreations,
}: {
  recentCreations: Creation[];
}) {
  const [photoModalOpen, setPhotoModalOpen] = useState(false);
  const [translateModalOpen, setTranslateModalOpen] = useState(false);
  const [changeVideoAudioOpen, setChangeVideoAudioOpen] = useState(false);
  const [playingVideoId, setPlayingVideoId] = useState<string | null>(null);
  const videoRefs = useRef<Record<string, HTMLVideoElement | null>>({});
  const [durations, setDurations] = useState<Record<string, number>>({});
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editingTitle, setEditingTitle] = useState("");
  const [editingType, setEditingType] = useState<string | null>(null);

  const [isRefreshing, setIsRefreshing] = useState(false);
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (searchParams.get("purchase_success") === "true") {
      toast.success(
        "Purchase successful. Your credits will be available shortly. Please refresh the page to view your updated balance.",
        { duration: 10000 },
      );
      router.replace(pathname);
    }
  }, [searchParams, router, pathname]);

  useEffect(() => {
    recentCreations.forEach((creation) => {
      if (creation.videoUrl && creation.status === "completed") {
        getVideoDuration(creation.videoUrl)
          .then((duration) => {
            setDurations((prev) => ({ ...prev, [creation.id]: duration }));
          })
          .catch(console.error);
      }
    });
  }, [recentCreations]);

  const handlePlayPause = async (id: string) => {
    const video = videoRefs.current[id];
    if (!video) return;

    if (video.paused) {
      if (playingVideoId && playingVideoId !== id) {
        videoRefs.current[playingVideoId]?.pause();
      }
      await video.play();
    } else {
      video.pause();
    }
  };

  const handleTitleUpdate = async () => {
    if (!editingId || !editingTitle || !editingType) {
      setEditingId(null);
      return;
    }

    await updateCreationName(editingId, editingTitle, editingType);
    setEditingId(null);
    setEditingType(null);
  };

  return (
    <div className="p-8">
      <h2 className="mb-6 text-lg font-semibold">Create something new</h2>
      <div className="mb-12 flex flex-wrap gap-4">
        {features.map((feature) => (
          <div
            key={feature.label}
            className="group relative flex min-w-80 cursor-pointer items-center gap-4 rounded-lg bg-white p-2 transition-all duration-300"
            onClick={() => {
              if (feature.label === "Photo to Video with Portrait Avatar") {
                setPhotoModalOpen(true);
              }
              if (feature.label === "Translate Video") {
                setTranslateModalOpen(true);
              }
              if (feature.label === "Change Video Audio") {
                setChangeVideoAudioOpen(true);
              }
            }}
          >
            <div
              className={`flex items-center justify-center rounded-lg p-3 ${feature.color}`}
            >
              <feature.icon className="h-5 w-5" />
            </div>
            <div className="relative flex w-full flex-col justify-center">
              <div className="text-sm font-medium text-gray-900 transition-all duration-300 group-hover:-translate-y-2.5">
                {feature.label}
              </div>
              <div className="pointer-events-none absolute top-3 left-0 text-xs text-gray-500 opacity-0 transition-opacity duration-300 group-hover:opacity-100">
                {feature.description}
              </div>
            </div>
            <span className="pointer-events-none absolute inset-0 rounded-lg border border-transparent opacity-0 shadow transition-all duration-300 group-hover:border-gray-200 group-hover:opacity-100 group-hover:shadow-md"></span>
          </div>
        ))}
      </div>

      {recentCreations.length > 0 && (
        <>
          <div className="mb-4 flex items-center gap-1">
            <h2 className="text-lg font-semibold">Recent creations</h2>
            <Button
              variant="ghost"
              onClick={() => {
                if (isRefreshing) return;
                setIsRefreshing(true);
                router.refresh();
                setTimeout(() => {
                  setIsRefreshing(false);
                }, 1000);
              }}
            >
              <RefreshCw className={isRefreshing ? "animate-spin" : ""} />
            </Button>
          </div>
          <div className="flex flex-wrap gap-6 pb-4">
            {recentCreations.map((item) => (
              <div
                key={item.id}
                className="relative w-64 flex-shrink-0 cursor-pointer overflow-hidden rounded-sm p-2 transition-all duration-300 hover:-translate-y-1 hover:shadow-md"
              >
                <div
                  onClick={() => handlePlayPause(item.id)}
                  className="group relative h-36 w-full rounded-sm bg-gray-900"
                >
                  {item.status === "completed" && item.videoUrl ? (
                    <>
                      <video
                        ref={(el) => {
                          videoRefs.current[item.id] = el;
                        }}
                        className={`h-full w-full rounded-sm transition-all duration-300 ${playingVideoId === item.id ? "object-contain" : "object-cover object-top"}`}
                        src={item.videoUrl}
                        onPlay={() => setPlayingVideoId(item.id)}
                        onPause={() => setPlayingVideoId(null)}
                        onEnded={() => setPlayingVideoId(null)}
                      />
                      {playingVideoId !== item.id && (
                        <div className="pointer-events-none absolute inset-0 flex items-center justify-center bg-black/20 opacity-0 transition-opacity group-hover:opacity-100">
                          <div className="flex h-12 w-12 items-center justify-center rounded-full bg-black/60">
                            <Play className="h-6 w-6 text-white" />
                          </div>
                        </div>
                      )}
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          window.open(item.videoUrl!, "_blank");
                        }}
                        className="absolute top-2 right-2 z-10 flex h-7 w-7 items-center justify-center rounded-full bg-black/60 text-white opacity-0 transition-opacity group-hover:opacity-100"
                      >
                        <Download className="h-4 w-4" />
                      </button>
                    </>
                  ) : (
                    <div className="flex h-full w-full flex-col items-center justify-center">
                      <div className="flex items-center gap-2 rounded-full bg-black/50 px-4 py-2 text-sm text-white">
                        {item.status === "processing" && "Currently processing"}
                        {item.status === "queued" && "In line for processing "}
                        {item.status === "failed" && "Generation failed"}
                        {item.status === "no credits" && "No credits"}
                        <Info className="h-4 w-4" />
                      </div>
                    </div>
                  )}
                  {playingVideoId !== item.id && (
                    <>
                      <span className="bg-opacity-60 absolute bottom-2 left-2 rounded bg-gray-800 px-2 py-0.5 text-xs text-white">
                        <Languages className="h-3 w-3" />
                      </span>
                      {durations[item.id] !== undefined && (
                        <span className="bg-opacity-60 absolute right-2 bottom-2 rounded bg-gray-800 px-2 py-0.5 text-xs text-white">
                          {Math.round(durations[item.id]!)}s
                        </span>
                      )}
                    </>
                  )}
                </div>
                <div className="p-3 pb-1">
                  {editingId === item.id ? (
                    <input
                      value={editingTitle}
                      onChange={(e) => setEditingTitle(e.target.value)}
                      autoFocus
                      onBlur={handleTitleUpdate}
                      onKeyDown={async (e) => {
                        if (e.key === "Enter") {
                          await handleTitleUpdate();
                        } else if (e.key === "Escape") {
                          setEditingId(null);
                        }
                      }}
                      type="text"
                      className="w-full rounded border border-blue-500 bg-white p-1 text-sm shadow-sm"
                    />
                  ) : (
                    <div
                      onClick={() => {
                        setEditingId(item.id);
                        setEditingTitle(item.title);
                        setEditingType(item.type);
                      }}
                      className="cursor-pointer text-sm"
                    >
                      {item.title}
                    </div>
                  )}
                  <div className="mt-1 text-xs font-light tracking-wide text-gray-400">
                    {item.date} • {item.type}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      <PhotoToVideoModal
        open={photoModalOpen}
        onOpenChange={setPhotoModalOpen}
      />
      <VideoTranslationModal
        open={translateModalOpen}
        onOpenChange={setTranslateModalOpen}
      />
      <ChangeVideoAudioModal
        open={changeVideoAudioOpen}
        onOpenChange={setChangeVideoAudioOpen}
      />
    </div>
  );
}
