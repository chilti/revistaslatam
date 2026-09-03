import es from './es';
import pt from './pt';
import en from './en';
import { useAppStore } from '../store';

export const dictionaries = { es, pt, en };

export function translate(key, lang = 'es', params = {}) {
  const dict = dictionaries[lang] || dictionaries.es;
  const keys = key.split('.');
  let val = dict;
  for (const k of keys) {
    if (val && typeof val === 'object' && k in val) {
      val = val[k];
    } else {
      // Fallback to Spanish
      let fallbackVal = dictionaries.es;
      for (const fk of keys) {
        if (fallbackVal && typeof fallbackVal === 'object' && fk in fallbackVal) {
          fallbackVal = fallbackVal[fk];
        } else {
          fallbackVal = key;
          break;
        }
      }
      val = fallbackVal;
      break;
    }
  }

  if (typeof val === 'string' && params) {
    let res = val;
    for (const [pk, pv] of Object.entries(params)) {
      res = res.replace(new RegExp(`\\{${pk}\\}`, 'g'), String(pv));
    }
    return res;
  }
  return typeof val === 'string' ? val : key;
}

export function useTranslation() {
  const { language = 'es', setLanguage } = useAppStore();

  const t = (key, params = {}) => translate(key, language, params);

  return {
    t,
    language,
    setLanguage,
    languages: [
      { code: 'es', label: 'Español', flag: '🇲🇽' },
      { code: 'pt', label: 'Português', flag: '🇧🇷' },
      { code: 'en', label: 'English', flag: '🇺🇸' }
    ]
  };
}

export default useTranslation;
