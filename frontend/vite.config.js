import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Vite configuration to enable React fast refresh and JSX support.
export default defineConfig({
  plugins: [react()],
  server: {
    open: true
  }
});
