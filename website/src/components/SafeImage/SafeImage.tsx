import React, { useEffect, useState } from "react";

type SafeImageProps = {
  src: string;
  alt?: string;
  className?: string;
  waitForLoad?: boolean;
};
export default function SafeImage(props: SafeImageProps) {
  const [isFailed, setIsFailed] = useState(false);
  const [isLoaded, setIsLoaded] = useState(false);

  useEffect(() => {
    setIsFailed(false);
    setIsLoaded(false);
  }, [props.src]);

  const imgSrc = isFailed || !props.src ? "/icon_not_found.png" : props.src;
  return (
    <img
      style={!isLoaded && props.waitForLoad ? { display: "none" } : {}}
      src={imgSrc}
      alt={props.alt}
      className={props.className}
      onError={() => setIsFailed(true)}
      onLoad={() => setIsLoaded(true)}
    />
  );
}
