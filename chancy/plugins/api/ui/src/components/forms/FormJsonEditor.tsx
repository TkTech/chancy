interface FormJsonEditorProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  field: any;
  rows?: number;
}

/**
 * JSON editor component integrated with TanStack Form
 * Uses textarea for editing, validates JSON on blur
 */
export function FormJsonEditor({ field, rows = 10 }: FormJsonEditorProps) {
  return (
    <textarea
      className={`form-control font-monospace code-font-sm ${field.state.meta.errors.length > 0 ? 'is-invalid' : ''}`}
      rows={rows}
      value={field.state.value ?? '{}'}
      onChange={(e) => field.handleChange(e.target.value)}
      onBlur={field.handleBlur}
    />
  );
}
