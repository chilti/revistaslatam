import axios from 'axios';

// Detect base path dynamically so API calls work seamlessly with /revistaslatam/ or root /
const pathname = typeof window !== 'undefined' ? window.location.pathname : '';
const basePath = pathname.includes('/revistaslatam') ? '/revistaslatam/api' : '/api';

const api = axios.create({
  baseURL: basePath,
  timeout: 30000,
});

export default api;
