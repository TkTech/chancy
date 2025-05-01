import { ReactNode, useCallback, useState } from "react";

/**
 * Hook to copy text to the clipboard.
 */
export function useCopy(): [boolean, (text: string) => void] {
  const [copied, setCopied] = useState(false);

  const onCopy = useCallback(
    (text: string) => {
      navigator.clipboard.writeText(text).then(() => {
        setCopied(true);
        setTimeout(() => {
          setCopied(false);
        }, 500);
      });
    },
    [],
  );

  return [copied, onCopy];
}

interface CopyButtonProps {
  className?: string;
  onDoCopy: () => string;
  children?: ReactNode;
}

/**
 * Component to copy text to the clipboard.
 */
export function CopyButton({ className, onDoCopy, children }: CopyButtonProps) {
  const [copied, onCopy] = useCopy();

  return (
    <button className={className} onClick={() => onCopy(onDoCopy())} disabled={copied}>
      {copied ? "Copied!" : children || "Copy"}
    </button>
  );
}

interface CopyTextProps {
  text: string;
  children: ReactNode;
}

/**
 * Component to copy text to the clipboard.
 */
export function CopyText({ text, children }: CopyTextProps) {
  const [copied, onCopy] = useCopy();

  const onClick = useCallback((e: React.MouseEvent) => {
    onCopy(text);
    e.stopPropagation();
  }, [onCopy, text]);

  return (
    <span onClick={onClick} style={{ cursor: "pointer" }}>
      {copied ? "Copied!" : children}
    </span>
  );
}