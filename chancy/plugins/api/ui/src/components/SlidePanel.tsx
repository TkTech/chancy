import React from 'react';

interface SlidePanelProps {
  isOpen: boolean;
  onClose: () => void;
  children: React.ReactNode;
  title: string;
  width?: string;
}

export function SlidePanel({ isOpen, onClose, children, title }: SlidePanelProps) {
  return (
    <>
      {isOpen && (
        <div 
          className="position-fixed top-0 start-0 w-100 h-100 bg-dark bg-opacity-50" 
          style={{ zIndex: 1040 }}
          onClick={onClose}
        />
      )}
      
      <div
        className={[
          "position-fixed",
          "top-0",
          "end-0",
          "vh-100",
          "shadow-lg",
          "overflow-auto",
          "transition-all",
          "mw-100",
          "w-75",
          "w-md-60",
          "w-lg-50",
          "w-xl-40",
          "border-start",
        ].join(" ")}
        style={{
          transform: isOpen ? 'translateX(0)' : 'translateX(100%)',
          transition: 'transform 0.3s ease-in-out',
          zIndex: 1050,
          backgroundColor: "var(--bs-body-bg)",
          borderColor: "var(--bs-border-color)",
        }}
      >
        <div className="d-flex justify-content-between align-items-center p-3 border-bottom">
          <h5 className="mb-0">{title}</h5>
          <button 
            type="button" 
            className="btn-close" 
            aria-label="Close" 
            onClick={onClose}
          />
        </div>
        <div className="p-3 overflow-auto" style={{ height: 'calc(100% - 60px)' }}>
          {children}
        </div>
      </div>
    </>
  );
}