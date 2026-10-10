import type { FieldControlProps, FormFieldState } from './FormField';

interface FormJsonEditorProps extends FieldControlProps {
  field: FormFieldState<string | Record<string, unknown> | undefined, string>;
  rows?: number;
}

export function FormJsonEditor({ field, rows = 6, ...control }: FormJsonEditorProps) {
  return <textarea
    {...control}
    className={`form-control font-monospace code-font-sm ${control['aria-invalid'] ? 'is-invalid' : ''}`}
    rows={rows}
    value={typeof field.state.value === 'string' ? field.state.value : JSON.stringify(field.state.value ?? {}, null, 2)}
    onChange={event => field.handleChange(event.target.value)}
    onBlur={field.handleBlur}
  />;
}
