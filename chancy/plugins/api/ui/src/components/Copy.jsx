import { useCallback, useState } from "react";

/**
 * Hook to copy text to the clipboard.
 *
 * @returns {(boolean|(function(*): void)|*)[]}
 */
export function useCopy() {
  const [copied, setCopied] = useState(false);

  const onCopy = useCallback(
    (text) => {
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

/**
 * Component to copy text to the clipboard.
 *
 * @param className
 * @param onDoCopy
 * @param children
 * @returns {JSX.Element}
 * @constructor
 */
export function CopyButton({ className, onDoCopy, children }) {
  const [copied, onCopy] = useCopy();

  return (
    <button className={className} onClick={() => onCopy(onDoCopy())} disabled={copied}>
      {copied ? "Copied!" : children || "Copy"}
    </button>
  );
}


/**
 * Component to copy text to the clipboard.
 *
 * @param text
 * @param children
 * @returns {JSX.Element}
 * @constructor
 */
export function CopyText({ text, children }) {
  const [copied, onCopy] = useCopy();

  const onClick = useCallback((e) => {
    onCopy(text);
    e.stopPropagation();
  }, [onCopy, text]);

  return (
    <span onClick={onClick} style={{ cursor: "pointer" }}>
      {copied ? "Copied!" : children}
    </span>
  );
}
