import axios from 'axios';

// Detect base path dynamically so API calls work seamlessly with /revistaslatam/ or root /
const getBaseUrl = () => {
  if (typeof window !== 'undefined' && window.location.pathname.includes('/revistaslatam')) {
    return '/revistaslatam/api';
  }
  return '/api';
};

const api = axios.create({
  baseURL: getBaseUrl(),
  timeout: 30000,
});

export default api;
