// navigation.js - Directional View Transition Support for MPA

(function() {
  'use strict';

  // Use Navigation API if available (Chrome 102+), fallback to popstate
  if (window.navigation) {
    // Modern Navigation API
    navigation.addEventListener('navigate', (event) => {
      if (!event.canIntercept || event.hashChange) return;
      
      const direction = event.navigationType;
      
      if (direction === 'push' || direction === 'replace') {
        document.documentElement.classList.add('nav-forward');
        document.documentElement.classList.remove('nav-back');
      } else if (direction === 'traverse') {
        // Check if going back or forward in history
        const fromIndex = navigation.currentEntry?.index ?? 0;
        const toIndex = event.destination?.index ?? 0;
        
        if (toIndex < fromIndex) {
          document.documentElement.classList.add('nav-back');
          document.documentElement.classList.remove('nav-forward');
        } else {
          document.documentElement.classList.add('nav-forward');
          document.documentElement.classList.remove('nav-back');
        }
      }
    });
  } else {
    // Fallback for browsers without Navigation API
    let historyIndex = history.state?.navIndex ?? 0;
    
    // Initialize history state
    if (history.state?.navIndex === undefined) {
      history.replaceState({ ...history.state, navIndex: historyIndex }, '');
    }

    // Intercept link clicks for forward navigation
    document.addEventListener('click', (e) => {
      const link = e.target.closest('a[href]');
      if (!link) return;
      
      const href = link.getAttribute('href');
      if (!href || href.startsWith('#') || href.startsWith('http') || 
          link.target === '_blank' || e.ctrlKey || e.metaKey) {
        return;
      }

      document.documentElement.classList.add('nav-forward');
      document.documentElement.classList.remove('nav-back');
    });

    // Detect back/forward via popstate
    window.addEventListener('popstate', (e) => {
      const newIndex = e.state?.navIndex ?? 0;
      
      if (newIndex < historyIndex) {
        document.documentElement.classList.add('nav-back');
        document.documentElement.classList.remove('nav-forward');
      } else {
        document.documentElement.classList.add('nav-forward');
        document.documentElement.classList.remove('nav-back');
      }
      
      historyIndex = newIndex;
    });

    // Increment history index on pushState
    const originalPushState = history.pushState;
    history.pushState = function(state, title, url) {
      historyIndex++;
      const newState = { ...state, navIndex: historyIndex };
      originalPushState.call(this, newState, title, url);
    };
  }

  // Clean up direction class after page loads
  window.addEventListener('pageshow', () => {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        document.documentElement.classList.remove('nav-forward', 'nav-back');
      });
    });
  });
})();
