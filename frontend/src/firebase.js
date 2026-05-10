// ============================================================
// firebase.js — Firebase configuration & initialization
// ============================================================

import { initializeApp } from 'firebase/app';
import { getAuth } from 'firebase/auth';

const firebaseConfig = {
  apiKey: "AIzaSyD9kzH8xAHNeTovnNZhJfl7uYQjoIHP61o",
  authDomain: "project3grupo1-2f40e.firebaseapp.com",
  projectId: "project3grupo1-2f40e",
  storageBucket: "project3grupo1-2f40e.firebasestorage.app",
  messagingSenderId: "585965542915",
  appId: "1:585965542915:web:b00bdf3edb92709107f836",
  measurementId: "G-7JZ6JQZ4KB",
};

const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export default app;
