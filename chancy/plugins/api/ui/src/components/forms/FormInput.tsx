import type { FieldControlProps, FormFieldState } from './FormField';

interface FormInputProps extends FieldControlProps {
  field: FormFieldState<string | number | null | undefined, string>;
  type?: 'text' | 'number';
  placeholder?: string;
  unit?: string;
  readOnly?: boolean;
}

export function FormInput({ field, type = 'text', placeholder, unit, readOnly, ...control }: FormInputProps) {
  const input = <input
    {...control}
    aria-describedby={[control['aria-describedby'], unit && `${control.id}-unit`].filter(Boolean).join(' ') || undefined}
    type={type}
    className={`form-control form-control-sm ${control['aria-invalid'] ? 'is-invalid' : ''}`}
    value={field.state.value ?? ''}
    onChange={event => field.handleChange(event.target.value)}
    onBlur={field.handleBlur}
    placeholder={placeholder}
    readOnly={readOnly}
  />;
  return unit ? <div className="input-group input-group-sm">{input}<span id={`${control.id}-unit`} className="input-group-text">{unit}</span></div> : input;
}
