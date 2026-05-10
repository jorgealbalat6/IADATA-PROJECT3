import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    open: true,
    proxy: {
      '/predict':    'http://localhost:8080',
      '/auth':       'http://localhost:8080',
      '/apartments': 'http://localhost:8080',
    },
  },
});
