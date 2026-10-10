import { useEffect, useRef } from 'react';
import { EditorView, minimalSetup } from 'codemirror';
import { json } from '@codemirror/lang-json';
import { EditorState } from '@codemirror/state';
import { oneDark } from '@codemirror/theme-one-dark';
import { syntaxHighlighting, defaultHighlightStyle } from '@codemirror/language';

interface JsonViewerProps {
  value: unknown;
  theme?: 'light' | 'dark';
}

export function JsonViewer({ value, theme = 'dark' }: JsonViewerProps) {
  const editorRef = useRef<HTMLDivElement>(null);
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!editorRef.current) return;

    const jsonString = JSON.stringify(value, null, 2);

    const extensions = [
      minimalSetup,
      json(),
      syntaxHighlighting(defaultHighlightStyle),
      EditorView.editable.of(false),
      EditorState.readOnly.of(true),
      EditorView.theme({
        '&': {
          fontSize: '0.875rem',
          border: 'none',
        },
        '.cm-scroller': {
          fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace',
        },
        '.cm-activeLine': {
          backgroundColor: 'transparent',
        },
        '.cm-activeLineGutter': {
          backgroundColor: 'transparent',
        },
      }),
    ];

    // Add dark theme if needed
    if (theme === 'dark') {
      extensions.push(
        oneDark,
        EditorView.theme({
          '&.cm-editor': {
            backgroundColor: 'transparent',
          },
          '.cm-gutters': {
            backgroundColor: 'transparent',
          },
        })
      );
    } else {
      // Light theme customizations
      extensions.push(
        EditorView.theme({
          '&.cm-editor': {
            backgroundColor: 'transparent',
            color: 'var(--bs-body-color)',
          },
          '.cm-content': {
            caretColor: 'var(--bs-body-color)',
          },
          '.cm-gutters': {
            backgroundColor: 'transparent',
            color: 'var(--bs-secondary-color)',
            border: 'none',
          },
        })
      );
    }

    const state = EditorState.create({
      doc: jsonString,
      extensions,
    });

    viewRef.current = new EditorView({
      state,
      parent: editorRef.current,
    });

    return () => {
      viewRef.current?.destroy();
      viewRef.current = null;
    };
  }, [value, theme]);

  return <div ref={editorRef} />;
}
