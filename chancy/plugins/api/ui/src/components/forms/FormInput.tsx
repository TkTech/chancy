interface FormInputProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  field: any;
  type?: 'text' | 'number';
  placeholder?: string;
  unit?: string;
}

/**
 * Text/Number input component integrated with TanStack Form
 */
export function FormInput({ field, type = 'text', placeholder, unit }: FormInputProps) {
  const input = (
    <input
      type={type}
      className={`form-control form-control-sm ${field.state.meta.errors.length > 0 ? 'is-invalid' : ''}`}
      value={field.state.value ?? ''}
      onChange={(e) => field.handleChange(e.target.value)}
      onBlur={field.handleBlur}
      placeholder={placeholder}
    />
  );

  if (unit) {
    return (
      <div className="input-group input-group-sm">
        {input}
        <span className="input-group-text">{unit}</span>
      </div>
    );
  }

  return input;
}
