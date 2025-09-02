import { useEffect, useRef, useState } from "react";

export function useAudioPlayer() {
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const [playingSrc, setPlayingSrc] = useState<string | null>(null);

  useEffect(() => {
    const audio = new Audio();
    audioRef.current = audio;

    const handleEnded = () => setPlayingSrc(null);
    audio.addEventListener("ended", handleEnded);

    return () => {
      audio.removeEventListener("ended", handleEnded);
      audio.pause();
    };
  }, []);

  const togglePlay = (src: string) => {
    const audio = audioRef.current;
    if (!audio) return;

    if (playingSrc === src) {
      audio.pause();
      setPlayingSrc(null);
    } else {
      audio.src = src;
      audio.play().catch((e) => console.error("Error playing audio"));
      setPlayingSrc(src);
    }
  };

  return {
    playingSrc,
    togglePlay,
  };
}
