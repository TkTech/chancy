interface FormCheckboxProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  field: any;
  label: string;
  id: string;
}

/**
 * Checkbox component integrated with TanStack Form
 */
export function FormCheckbox({ field, label, id }: FormCheckboxProps) {
  return (
    <div className="form-check form-switch">
      <input
        className="form-check-input"
        type="checkbox"
        checked={!!field.state.value}
        onChange={(e) => field.handleChange(e.target.checked)}
        onBlur={field.handleBlur}
        id={id}
      />
      <label className="form-check-label" htmlFor={id}>
        {field.state.value ? label : `${label} (Disabled)`}
      </label>
    </div>
  );
}
