import { ReactNode, useId } from 'react';

export interface FieldControlProps {
  id: string;
  'aria-describedby'?: string;
  'aria-invalid'?: boolean;
  'aria-required'?: boolean;
}

interface FormFieldProps {
  label: string;
  errors?: readonly ({ message: string } | undefined)[];
  help?: ReactNode;
  children: (props: FieldControlProps) => ReactNode;
  required?: boolean;
}

export function FormField({ label, errors, help, children, required }: FormFieldProps) {
  const id = useId();
  const error = errors?.map(issue => issue?.message).filter(Boolean).join(' ');
  const describedBy = [help && `${id}-help`, error && `${id}-error`].filter(Boolean).join(' ') || undefined;
  return (
    <div className="mb-3">
      <label htmlFor={id} className="form-label small text-muted fw-semibold text-uppercase d-block">
        {label}{required && <span className="text-danger ms-1" aria-hidden="true">*</span>}
      </label>
      {children({ id, 'aria-describedby': describedBy, 'aria-invalid': !!error, 'aria-required': required })}
      {help && <div id={`${id}-help`} className="form-text">{help}</div>}
      {error && <div id={`${id}-error`} className="invalid-feedback d-block">{error}</div>}
    </div>
  );
}

export interface FormFieldState<T, TChange = T> {
  state: { value: T };
  handleChange: (value: TChange) => void;
  handleBlur: () => void;
}
