# Chancy Color Palette

This is the official Chancy color palette for use across all branded materials including the dashboard, blog, documentation, and marketing.

## Brand Colors

### Chancy Pink (Primary Brand)
- **Base**: `#fad0d7` - Soft pink from logo, perfect for backgrounds and gentle emphasis
- **Dark**: `#f4a6b4` - Medium pink for interactive states
- **Emphasis**: `#e87d94` - Vibrant pink for strong accents and calls-to-action

### Chancy Blue (Primary Actions)
- **Base**: `#4a90e2` - Clear, accessible blue for primary buttons and links
- **Light**: `#6ba9f0` - Lighter variant for hover states and dark mode
- **Dark**: `#2b6cb0` - Deeper blue for pressed states

### Chancy Purple (Secondary Accent)
- **Base**: `#9b6dd6` - Sophisticated purple for info states and variety
- **Light**: `#b591e3` - Softer purple for dark mode
- **Dark**: `#7c4fb8` - Rich purple for emphasis

## Semantic Colors

- **Success**: `#28a745` - Standard green for success states
- **Warning**: `#f59e0b` - Amber for warnings and pending states
- **Danger**: `#dc3545` - Red for errors and destructive actions

## Light Theme

### Backgrounds
- **Primary**: `#ffffff` - Main background
- **Secondary**: `#f8f9fa` - Sidebar, cards
- **Tertiary**: `#e9ecef` - Subtle backgrounds

### Text
- **Primary**: `#1a1d23` - Main text, high contrast
- **Secondary**: `#525560` - Supporting text
- **Tertiary**: `#6c757d` - De-emphasized text

### Borders
- **Default**: `#dee2e6` - Standard borders
- **Subtle**: `#e9ecef` - Gentle dividers

## Dark Theme

### Backgrounds
- **Primary**: `#0d0e12` - Deep, rich main background
- **Secondary**: `#16171d` - Sidebar, cards, surfaces
- **Tertiary**: `#1e1f26` - Elevated surfaces, card headers

### Text
- **Primary**: `#f0f1f3` - Main text, bright and readable
- **Secondary**: `#b4b6bb` - Supporting text
- **Tertiary**: `#888a91` - De-emphasized text

### Borders
- **Default**: `#2a2b33` - Standard borders
- **Subtle**: `#1e1f26` - Gentle dividers

## Usage Examples

### CSS/SCSS
```scss
// Direct colors
color: #fad0d7;  // Chancy pink base

// CSS custom properties (available at runtime)
color: var(--chancy-pink);
color: var(--chancy-pink-dark);
color: var(--chancy-pink-emphasis);

// Utility classes
.text-chancy-pink { color: var(--chancy-pink-emphasis); }
.bg-chancy-pink { background-color: var(--chancy-pink); }
.badge-pink { /* Custom pink badge */ }
```

### Tailwind Config (for blog)
```js
module.exports = {
  theme: {
    extend: {
      colors: {
        'chancy-pink': {
          DEFAULT: '#fad0d7',
          dark: '#f4a6b4',
          emphasis: '#e87d94',
        },
        'chancy-blue': {
          DEFAULT: '#4a90e2',
          light: '#6ba9f0',
          dark: '#2b6cb0',
        },
        'chancy-purple': {
          DEFAULT: '#9b6dd6',
          light: '#b591e3',
          dark: '#7c4fb8',
        },
      },
    },
  },
}
```

## Design Principles

1. **Pink as Subtle Emphasis**: Use Chancy pink sparingly for highlights, not as primary action color
2. **Blue for Actions**: Primary interactive elements use Chancy blue for clear affordance
3. **High Contrast**: Both themes maintain WCAG AA contrast ratios for accessibility
4. **Consistent Across Themes**: The pink accent maintains its character in both light and dark modes
5. **Progressive Disclosure**: Use text hierarchy (primary/secondary/tertiary) to guide attention
