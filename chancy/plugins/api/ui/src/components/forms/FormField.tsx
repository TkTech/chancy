import { ReactNode } from 'react';

interface FormFieldProps {
  label: string;
  error?: string;
  children: ReactNode;
  required?: boolean;
}

/**
 * Wrapper component for form fields with consistent styling and error display
 */
export function FormField({ label, error, children, required }: FormFieldProps) {
  return (
    <div className="mb-3">
      <label className="form-label small text-muted fw-semibold text-uppercase d-block">
        {label}
        {required && <span className="text-danger ms-1">*</span>}
      </label>
      <div>
        {children}
      </div>
      {error && (
        <div className="invalid-feedback d-block">
          {error}
        </div>
      )}
    </div>
  );
}
