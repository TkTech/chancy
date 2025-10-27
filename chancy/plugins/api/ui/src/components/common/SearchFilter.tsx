import React, { useState, useRef, useEffect } from 'react';

export type FilterTriple = [string, string, string]; // [key, operator, value]

export interface FieldConfig {
  label: string;
  description?: string; // Description shown in dropdown
  type: 'text' | 'autocomplete' | 'numeric';
  operators: string[]; // e.g., ['=', '~'] for text, ['=', '>', '<', '>=', '<='] for numeric
  getSuggestions?: (query: string) => Promise<string[]>;
}

export interface SearchFilterProps {
  fields: Record<string, FieldConfig>;
  value: FilterTriple[];
  onChange: (filters: FilterTriple[]) => void;
  placeholder?: string;
}

type DropdownState = {
  type: 'field' | 'operator' | 'value';
  suggestions: string[];
  selectedIndex: number;
  fieldKey?: string;
  operator?: string;
};

export function SearchFilter({ fields, value, onChange, placeholder = 'Add filter...' }: SearchFilterProps) {
  const [inputValue, setInputValue] = useState('');
  const [dropdown, setDropdown] = useState<DropdownState | null>(null);
  const [loading, setLoading] = useState(false);
  const [partialFilter, setPartialFilter] = useState<{ fieldKey?: string; operator?: string } | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Show field suggestions when input is focused or typing
  useEffect(() => {
    // Only auto-update dropdown when not in a partial filter (or only at field selection stage)
    if (partialFilter?.fieldKey) {
      // Already selected a field, don't interfere with operator/value stages
      return;
    }

    if (inputValue === '' && document.activeElement === inputRef.current) {
      // Show available fields
      setDropdown({
        type: 'field',
        suggestions: Object.keys(fields),
        selectedIndex: 0,
      });
    } else if (inputValue.trim()) {
      // Filter fields by input
      const matchingFields = Object.keys(fields).filter(key =>
        key.toLowerCase().includes(inputValue.toLowerCase()) ||
        fields[key].label.toLowerCase().includes(inputValue.toLowerCase())
      );
      if (matchingFields.length > 0) {
        setDropdown({
          type: 'field',
          suggestions: matchingFields,
          selectedIndex: 0,
        });
      } else {
        setDropdown(null);
      }
    } else {
      setDropdown(null);
    }
  }, [inputValue, fields, partialFilter]);

  const selectField = (fieldKey: string) => {
    const field = fields[fieldKey];
    setPartialFilter({ fieldKey });
    if (field.operators.length === 1) {
      // Only one operator, skip to value input
      selectOperator(fieldKey, field.operators[0]);
    } else {
      // Show operator dropdown
      setDropdown({
        type: 'operator',
        suggestions: field.operators,
        selectedIndex: 0,
        fieldKey,
      });
      setInputValue('');
    }
  };

  const selectOperator = async (fieldKey: string, operator: string) => {
    const field = fields[fieldKey];
    setPartialFilter({ fieldKey, operator });

    // If field has autocomplete, fetch suggestions
    if (field.getSuggestions) {
      setLoading(true);
      try {
        const suggestions = await field.getSuggestions('');
        setDropdown({
          type: 'value',
          suggestions,
          selectedIndex: 0,
          fieldKey,
          operator,
        });
      } catch (error) {
        console.error('Failed to fetch suggestions:', error);
        setDropdown({
          type: 'value',
          suggestions: [],
          selectedIndex: 0,
          fieldKey,
          operator,
        });
      } finally {
        setLoading(false);
      }
    } else {
      // No autocomplete, show empty value input
      setDropdown({
        type: 'value',
        suggestions: [],
        selectedIndex: 0,
        fieldKey,
        operator,
      });
    }
    setInputValue('');
  };

  const selectValue = (fieldKey: string, operator: string, selectedValue: string) => {
    // Add the filter
    onChange([...value, [fieldKey, operator, selectedValue]]);
    // Reset input and partial filter
    setInputValue('');
    setDropdown(null);
    setPartialFilter(null);
    inputRef.current?.focus();
  };

  const removeFilter = (index: number) => {
    onChange(value.filter((_, i) => i !== index));
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    // Handle backspace
    if (e.key === 'Backspace' && inputValue === '') {
      if (partialFilter) {
        e.preventDefault();
        // Go back a stage
        if (partialFilter.operator) {
          // Go back to operator selection
          const field = fields[partialFilter.fieldKey!];
          if (field.operators.length > 1) {
            setPartialFilter({ fieldKey: partialFilter.fieldKey });
            setDropdown({
              type: 'operator',
              suggestions: field.operators,
              selectedIndex: 0,
              fieldKey: partialFilter.fieldKey!,
            });
          } else {
            // Only one operator, go back to field selection
            setPartialFilter(null);
            setDropdown({
              type: 'field',
              suggestions: Object.keys(fields),
              selectedIndex: 0,
            });
          }
        } else if (partialFilter.fieldKey) {
          // Go back to field selection
          setPartialFilter(null);
          setDropdown({
            type: 'field',
            suggestions: Object.keys(fields),
            selectedIndex: 0,
          });
        }
      } else if (value.length > 0) {
        // Remove last complete filter
        removeFilter(value.length - 1);
      }
      return;
    }

    if (!dropdown) {
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setDropdown({
          ...dropdown,
          selectedIndex: Math.min(dropdown.selectedIndex + 1, dropdown.suggestions.length - 1),
        });
        break;
      case 'ArrowUp':
        e.preventDefault();
        setDropdown({
          ...dropdown,
          selectedIndex: Math.max(dropdown.selectedIndex - 1, 0),
        });
        break;
      case 'Enter':
        e.preventDefault();
        if (dropdown.type === 'field') {
          if (dropdown.suggestions.length > 0) {
            // Use the currently selected suggestion from the filtered list
            const selected = dropdown.suggestions[dropdown.selectedIndex];
            selectField(selected);
          }
        } else if (dropdown.type === 'operator' && dropdown.fieldKey) {
          if (dropdown.suggestions.length > 0) {
            // Use the currently selected operator
            const selected = dropdown.suggestions[dropdown.selectedIndex];
            selectOperator(dropdown.fieldKey, selected);
          }
        } else if (dropdown.type === 'value' && dropdown.fieldKey && dropdown.operator) {
          // Allow typing arbitrary value or selecting from suggestions
          const valueToUse = inputValue.trim() || (dropdown.suggestions.length > 0 ? dropdown.suggestions[dropdown.selectedIndex] : '');
          if (valueToUse) {
            selectValue(dropdown.fieldKey, dropdown.operator, valueToUse);
          }
        }
        break;
      case 'Escape':
        e.preventDefault();
        setDropdown(null);
        setInputValue('');
        setPartialFilter(null);
        break;
    }
  };

  const handleInputChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    setInputValue(newValue);

    // If we're in value input mode with autocomplete, filter suggestions
    if (dropdown?.type === 'value' && dropdown.fieldKey && fields[dropdown.fieldKey].getSuggestions) {
      setLoading(true);
      try {
        const suggestions = await fields[dropdown.fieldKey].getSuggestions!(newValue);
        setDropdown({
          ...dropdown,
          suggestions,
          selectedIndex: 0,
        });
      } catch (error) {
        console.error('Failed to fetch suggestions:', error);
      } finally {
        setLoading(false);
      }
    }
  };

  const handleInputBlur = (_e: React.FocusEvent<HTMLInputElement>) => {
    // Delay to allow clicking on dropdown items
    setTimeout(() => {
      if (!dropdownRef.current?.contains(document.activeElement)) {
        // Hide dropdown but keep partial filter and input value
        setDropdown(null);
      }
    }, 200);
  };

  // Render operator symbol in a more friendly way
  const getOperatorLabel = (op: string): string => {
    const labels: Record<string, string> = {
      '=': '=',
      '~': '≈',
      '>': '>',
      '<': '<',
      '>=': '≥',
      '<=': '≤',
    };
    return labels[op] || op;
  };

  // Get operator description
  const getOperatorDescription = (op: string): string => {
    const descriptions: Record<string, string> = {
      '=': 'equals',
      '~': 'contains (case-insensitive)',
      '>': 'greater than',
      '<': 'less than',
      '>=': 'greater than or equal to',
      '<=': 'less than or equal to',
    };
    return descriptions[op] || '';
  };

  return (
    <div className="search-filter">
      <div className="search-filter-container">
        {/* Display active filters as chips */}
        {value.map((filter, index) => {
          const [key, operator, val] = filter;
          const field = fields[key];
          return (
            <div key={index} className="filter-chip">
              <span className="filter-chip-content">
                <span className="filter-chip-field">{field?.label || key}</span>
                <span className="filter-chip-operator">{getOperatorLabel(operator)}</span>
                <span className="filter-chip-value">{val}</span>
              </span>
              <button
                type="button"
                className="filter-chip-remove"
                onClick={() => removeFilter(index)}
                aria-label="Remove filter"
              >
                ×
              </button>
            </div>
          );
        })}

        {/* Display partial filter being built */}
        {partialFilter && (
          <div className="filter-chip filter-chip-partial">
            <span className="filter-chip-content">
              {partialFilter.fieldKey && (
                <span className="filter-chip-field">{fields[partialFilter.fieldKey]?.label || partialFilter.fieldKey}</span>
              )}
              {partialFilter.operator && (
                <span className="filter-chip-operator">{getOperatorLabel(partialFilter.operator)}</span>
              )}
              <span className="filter-chip-value filter-chip-incomplete">...</span>
            </span>
          </div>
        )}

        {/* Input for new filters */}
        <div className="search-filter-input-wrapper">
          <input
            ref={inputRef}
            type="text"
            className="search-filter-input"
            value={inputValue}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            onBlur={handleInputBlur}
            onFocus={() => {
              if (inputValue === '' && !dropdown) {
                setDropdown({
                  type: 'field',
                  suggestions: Object.keys(fields),
                  selectedIndex: 0,
                });
              }
            }}
            placeholder={
              value.length === 0 && !partialFilter
                ? placeholder
                : dropdown?.type === 'value' && partialFilter?.fieldKey
                  ? `Enter ${fields[partialFilter.fieldKey]?.label.toLowerCase()} value...`
                  : ''
            }
          />
          {loading && <span className="search-filter-loading">...</span>}
        </div>
      </div>

      {/* Dropdown suggestions */}
      {dropdown && dropdown.suggestions.length > 0 && (
        <div ref={dropdownRef} className="search-filter-dropdown">
          {dropdown.suggestions.map((suggestion, index) => {
            const isSelected = index === dropdown.selectedIndex;
            let label = suggestion;
            let description = '';

            if (dropdown.type === 'field') {
              const field = fields[suggestion];
              label = field.label;
              description = field.description || `Filter by ${suggestion}`;
            } else if (dropdown.type === 'operator') {
              label = getOperatorLabel(suggestion);
              description = getOperatorDescription(suggestion);
            }

            return (
              <div
                key={index}
                className={`search-filter-dropdown-item ${isSelected ? 'selected' : ''}`}
                onMouseDown={(e) => {
                  e.preventDefault();
                  if (dropdown.type === 'field') {
                    selectField(suggestion);
                  } else if (dropdown.type === 'operator' && dropdown.fieldKey) {
                    selectOperator(dropdown.fieldKey, suggestion);
                  } else if (dropdown.type === 'value' && dropdown.fieldKey && dropdown.operator) {
                    selectValue(dropdown.fieldKey, dropdown.operator, suggestion);
                  }
                }}
              >
                <div className="search-filter-dropdown-item-label">{label}</div>
                {description && <div className="search-filter-dropdown-item-description">{description}</div>}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
