import { useState } from 'react';

import type { FieldControlProps, FormFieldState } from './FormField';

interface FormTagInputProps extends FieldControlProps {
  field: FormFieldState<string[] | undefined, string[]>;
}

/**
 * Tag input component integrated with TanStack Form
 */
export function FormTagInput({ field, ...control }: FormTagInputProps) {
  const [inputValue, setInputValue] = useState('');
  const tags = field.state.value || [];

  const addTag = () => {
    const newTag = inputValue.trim();
    if (newTag && !tags.includes(newTag)) {
      field.handleChange([...tags, newTag]);
      setInputValue('');
    }
  };

  const removeTag = (tagToRemove: string) => {
    field.handleChange(tags.filter((tag: string) => tag !== tagToRemove));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addTag();
    }
  };

  return (
    <div>
      <div className="mb-2">
        {tags.length === 0 ? (
          <span className="text-muted small">No tags</span>
        ) : (
          tags.map((tag: string) => (
            <span key={tag} className="badge bg-primary me-1 mb-1">
              {tag}
              <button
                type="button"
                className="btn-close btn-close-white ms-1"
                style={{ fontSize: '0.5rem' }}
                onClick={() => removeTag(tag)}
                aria-label={`Remove ${tag}`}
              />
            </span>
          ))
        )}
      </div>
      <div className="input-group input-group-sm">
        <input
          {...control}
          onBlur={() => { addTag(); field.handleBlur(); }}
          type="text"
          className="form-control"
          placeholder="Add tag..."
          value={inputValue}
          onChange={e => setInputValue(e.target.value)}
          onKeyDown={handleKeyDown}
        />
        <button className="btn btn-outline-secondary" type="button" onClick={addTag}>
          Add
        </button>
      </div>
    </div>
  );
}
