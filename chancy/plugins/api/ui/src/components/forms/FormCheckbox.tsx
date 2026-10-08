import type { FieldControlProps, FormFieldState } from './FormField';

interface FormCheckboxProps extends FieldControlProps {
  field: FormFieldState<boolean | undefined, boolean>;
}

export function FormCheckbox({ field, ...control }: FormCheckboxProps) {
  return <div className="form-check form-switch">
    <input {...control} className="form-check-input" type="checkbox"
      checked={!!field.state.value} onChange={event => field.handleChange(event.target.checked)} onBlur={field.handleBlur} />
    <span aria-hidden="true">{field.state.value ? 'Enabled' : 'Disabled'}</span>
  </div>;
}
