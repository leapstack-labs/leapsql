import type { FunctionComponent } from 'preact';
import { useState, useEffect } from 'preact/hooks';

export const ThemeSwitcher: FunctionComponent = () => {
  const [themes, setThemes] = useState<string[]>([]);
  const [current, setCurrent] = useState<string>(() => 
    localStorage.getItem('dev-theme') || 'vercel'
  );
  const [loading, setLoading] = useState(false);

  // Fetch available themes on mount
  useEffect(() => {
    fetch('/themes')
      .then(r => r.json())
      .then(setThemes)
      .catch(console.error);
  }, []);

  // Load theme CSS when selection changes
  useEffect(() => {
    if (!current) return;
    
    setLoading(true);
    fetch(`/themes/${current}.css`)
      .then(r => r.text())
      .then(css => {
        // Find or create style element
        let style = document.getElementById('dev-theme') as HTMLStyleElement;
        if (!style) {
          style = document.createElement('style');
          style.id = 'dev-theme';
          document.head.appendChild(style);
        }
        style.textContent = css;
        localStorage.setItem('dev-theme', current);
        setLoading(false);
      })
      .catch(err => {
        console.error('Failed to load theme:', err);
        setLoading(false);
      });
  }, [current]);

  const handleChange = (e: Event) => {
    const target = e.target as HTMLSelectElement;
    setCurrent(target.value);
  };

  if (themes.length === 0) return null;

  return (
    <div class="theme-switcher">
      <label class="theme-switcher-label">Theme:</label>
      <select 
        class="theme-switcher-select"
        value={current} 
        onChange={handleChange}
        disabled={loading}
      >
        {themes.map(theme => (
          <option key={theme} value={theme}>
            {theme}
          </option>
        ))}
      </select>
    </div>
  );
};
