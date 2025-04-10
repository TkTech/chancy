import { createContext, useContext, useState, ReactNode, useEffect } from 'react';
import { SlidePanel } from './SlidePanel';

interface PanelItem {
  id: string;
  title: string;
  content: ReactNode;
  width?: string;
}

interface SlidePanelContextType {
  panels: PanelItem[];
  openPanel: (panel: Omit<PanelItem, 'id'>) => string;
  closePanel: (id: string) => void;
  closeAllPanels: () => void;
}

const SlidePanelContext = createContext<SlidePanelContextType | undefined>(undefined);

export function SlidePanelProvider({ children }: { children: ReactNode }) {
  const [panels, setPanels] = useState<(PanelItem & { isVisible: boolean })[]>([]);
  
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && panels.length > 0) {
        closePanel(panels[panels.length - 1].id);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [panels]);

  const openPanel = (panel: Omit<PanelItem, 'id'>) => {
    const id = crypto.randomUUID();
    setPanels(prev => [...prev, { ...panel, id, isVisible: false }]);
    setTimeout(() => {
      setPanels(prev => prev.map(p => p.id === id ? { ...p, isVisible: true } : p));
    }, 50);
    
    return id;
  };

  const closePanel = (id: string) => {
    setPanels(prev => prev.map(p => p.id === id ? { ...p, isVisible: false } : p));
    setTimeout(() => {
      setPanels(prev => prev.filter(panel => panel.id !== id));
    }, 300);
  };

  const closeAllPanels = () => {
    setPanels(prev => prev.map(p => ({ ...p, isVisible: false })));
    setTimeout(() => {
      setPanels([]);
    }, 300);
  };

  return (
    <SlidePanelContext.Provider value={{ panels, openPanel, closePanel, closeAllPanels }}>
      {children}
      
      {panels.some(p => p.isVisible) && (
        <div 
          className="position-fixed top-0 start-0 w-100 h-100 bg-dark bg-opacity-50" 
          style={{ 
            zIndex: 1040,
            opacity: 0.5,
            transition: 'opacity 0.3s ease-in-out, visibility 0.3s ease-in-out'
          }}
          onClick={() => {
            if (panels.length > 0) {
              closePanel(panels[panels.length - 1].id);
            }
          }}
        />
      )}
      
      {panels.map((panel, index) => (
        <SlidePanel
          key={panel.id}
          isOpen={panel.isVisible}
          onClose={() => closePanel(panel.id)}
          title={panel.title}
          maxWidth={panel.width}
          style={{
            zIndex: 1050 + index,
          }}
          hideOverlay={true}
        >
          {panel.content}
        </SlidePanel>
      ))}
    </SlidePanelContext.Provider>
  );
}

export function useSlidePanels() {
  const context = useContext(SlidePanelContext);
  if (!context) {
    throw new Error('useSlidePanels must be used within a SlidePanelProvider');
  }
  return context;
}