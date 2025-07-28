/**
 * Unit tests for common utility functions used across the metadata manager.
 * These tests focus on pure JavaScript functions without external dependencies.
 */

describe('String Utilities', () => {
  
  // Utility function for safe string operations
  function safeStringTrim(str) {
    if (typeof str !== 'string') return '';
    return str.trim();
  }
  
  function capitalizeFirst(str) {
    if (!str || typeof str !== 'string') return '';
    return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();
  }
  
  function truncateString(str, maxLength, suffix = '...') {
    if (!str || typeof str !== 'string') return '';
    if (str.length <= maxLength) return str;
    return str.substring(0, maxLength - suffix.length) + suffix;
  }
  
  test('safeStringTrim() handles various input types', () => {
    expect(safeStringTrim('  hello world  ')).toBe('hello world');
    expect(safeStringTrim('')).toBe('');
    expect(safeStringTrim(null)).toBe('');
    expect(safeStringTrim(undefined)).toBe('');
    expect(safeStringTrim(123)).toBe('');
    expect(safeStringTrim('   ')).toBe('');
  });
  
  test('capitalizeFirst() formats strings correctly', () => {
    expect(capitalizeFirst('hello')).toBe('Hello');
    expect(capitalizeFirst('HELLO')).toBe('Hello');
    expect(capitalizeFirst('hello world')).toBe('Hello world');
    expect(capitalizeFirst('')).toBe('');
    expect(capitalizeFirst(null)).toBe('');
    expect(capitalizeFirst(undefined)).toBe('');
  });
  
  test('truncateString() handles text truncation', () => {
    expect(truncateString('hello world', 5)).toBe('he...');
    expect(truncateString('hello', 10)).toBe('hello');
    expect(truncateString('hello world', 11)).toBe('hello world');
    expect(truncateString('hello world', 8, '…')).toBe('hello w…');
    expect(truncateString('', 5)).toBe('');
    expect(truncateString(null, 5)).toBe('');
  });
});

describe('Array Utilities', () => {
  
  function safeArrayFilter(arr, predicate) {
    if (!Array.isArray(arr)) return [];
    return arr.filter(predicate);
  }
  
  function groupBy(arr, keyFn) {
    if (!Array.isArray(arr)) return {};
    
    return arr.reduce((groups, item) => {
      const key = keyFn(item);
      if (!groups[key]) {
        groups[key] = [];
      }
      groups[key].push(item);
      return groups;
    }, {});
  }
  
  function uniqueBy(arr, keyFn) {
    if (!Array.isArray(arr)) return [];
    
    const seen = new Set();
    return arr.filter(item => {
      const key = keyFn(item);
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });
  }
  
  test('safeArrayFilter() handles various input types', () => {
    const numbers = [1, 2, 3, 4, 5];
    const isEven = n => n % 2 === 0;
    
    expect(safeArrayFilter(numbers, isEven)).toEqual([2, 4]);
    expect(safeArrayFilter([], isEven)).toEqual([]);
    expect(safeArrayFilter(null, isEven)).toEqual([]);
    expect(safeArrayFilter(undefined, isEven)).toEqual([]);
    expect(safeArrayFilter('not array', isEven)).toEqual([]);
  });
  
  test('groupBy() groups array items correctly', () => {
    const items = [
      { type: 'tag', name: 'tag1' },
      { type: 'domain', name: 'domain1' },
      { type: 'tag', name: 'tag2' },
      { type: 'domain', name: 'domain2' }
    ];
    
    const grouped = groupBy(items, item => item.type);
    
    expect(grouped).toHaveProperty('tag');
    expect(grouped).toHaveProperty('domain'); 
    expect(grouped.tag).toHaveLength(2);
    expect(grouped.domain).toHaveLength(2);
    expect(grouped.tag[0].name).toBe('tag1');
    
    // Edge cases
    expect(groupBy([], item => item.type)).toEqual({});
    expect(groupBy(null, item => item.type)).toEqual({});
  });
  
  test('uniqueBy() removes duplicates correctly', () => {
    const items = [
      { id: 1, name: 'item1' },
      { id: 2, name: 'item2' },
      { id: 1, name: 'item1-duplicate' },
      { id: 3, name: 'item3' }
    ];
    
    const unique = uniqueBy(items, item => item.id);
    
    expect(unique).toHaveLength(3);
    expect(unique[0]).toEqual({ id: 1, name: 'item1' });
    expect(unique[1]).toEqual({ id: 2, name: 'item2' });
    expect(unique[2]).toEqual({ id: 3, name: 'item3' });
    
    // Edge cases
    expect(uniqueBy([], item => item.id)).toEqual([]);
    expect(uniqueBy(null, item => item.id)).toEqual([]);
  });
});

describe('Object Utilities', () => {
  
  function safeObjectGet(obj, path, defaultValue = null) {
    if (!obj || typeof obj !== 'object') return defaultValue;
    
    const keys = path.split('.');
    let current = obj;
    
    for (const key of keys) {
      if (current == null || typeof current !== 'object') {
        return defaultValue;
      }
      current = current[key];
    }
    
    return current !== undefined ? current : defaultValue;
  }
  
  function deepClone(obj) {
    if (obj === null || typeof obj !== 'object') {
      return obj;
    }
    
    if (obj instanceof Date) {
      return new Date(obj.getTime());
    }
    
    if (Array.isArray(obj)) {
      return obj.map(item => deepClone(item));
    }
    
    const cloned = {};
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        cloned[key] = deepClone(obj[key]);
      }
    }
    
    return cloned;
  }
  
  function mergeObjects(...objects) {
    const result = {};
    
    for (const obj of objects) {
      if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
        Object.assign(result, obj);
      }
    }
    
    return result;
  }
  
  test('safeObjectGet() retrieves nested properties safely', () => {
    const obj = {
      user: {
        profile: {
          name: 'John Doe',
          settings: {
            theme: 'dark'
          }
        }
      }
    };
    
    expect(safeObjectGet(obj, 'user.profile.name')).toBe('John Doe');
    expect(safeObjectGet(obj, 'user.profile.settings.theme')).toBe('dark');
    expect(safeObjectGet(obj, 'user.profile.age', 25)).toBe(25);
    expect(safeObjectGet(obj, 'nonexistent.path')).toBe(null);
    expect(safeObjectGet(null, 'any.path')).toBe(null);
    expect(safeObjectGet(undefined, 'any.path')).toBe(null);
    expect(safeObjectGet('not object', 'any.path')).toBe(null);
  });
  
  test('deepClone() creates independent copies', () => {
    const original = {
      name: 'test',
      nested: {
        value: 42,
        array: [1, 2, { inner: 'value' }]
      },
      date: new Date('2023-01-01')
    };
    
    const cloned = deepClone(original);
    
    // Verify it's a different object
    expect(cloned).not.toBe(original);
    expect(cloned.nested).not.toBe(original.nested);
    expect(cloned.nested.array).not.toBe(original.nested.array);
    
    // Verify values are the same
    expect(cloned.name).toBe(original.name);
    expect(cloned.nested.value).toBe(original.nested.value);
    expect(cloned.date.getTime()).toBe(original.date.getTime());
    
    // Verify changes don't affect original
    cloned.nested.value = 99;
    expect(original.nested.value).toBe(42);
    
    // Edge cases
    expect(deepClone(null)).toBe(null);
    expect(deepClone(undefined)).toBe(undefined);
    expect(deepClone('string')).toBe('string');
    expect(deepClone(123)).toBe(123);
  });
  
  test('mergeObjects() combines objects correctly', () => {
    const obj1 = { a: 1, b: 2 };
    const obj2 = { b: 3, c: 4 };
    const obj3 = { c: 5, d: 6 };
    
    const merged = mergeObjects(obj1, obj2, obj3);
    
    expect(merged).toEqual({ a: 1, b: 3, c: 5, d: 6 });
    
    // Edge cases
    expect(mergeObjects()).toEqual({});
    expect(mergeObjects(null, undefined, obj1)).toEqual(obj1);
    expect(mergeObjects(obj1, 'not object', obj2)).toEqual({ a: 1, b: 3, c: 4 });
  });
});

describe('Date Utilities', () => {
  
  function formatDate(date, format = 'YYYY-MM-DD') {
    if (!(date instanceof Date) || isNaN(date)) {
      return '';
    }
    
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    
    switch (format) {
      case 'YYYY-MM-DD':
        return `${year}-${month}-${day}`;
      case 'DD/MM/YYYY':
        return `${day}/${month}/${year}`;
      case 'MM/DD/YYYY':
        return `${month}/${day}/${year}`;
      default:
        return `${year}-${month}-${day}`;
    }
  }
  
  function isValidDate(date) {
    return date instanceof Date && !isNaN(date);
  }
  
  function daysBetween(date1, date2) {
    if (!isValidDate(date1) || !isValidDate(date2)) {
      return 0;
    }
    
    const timeDiff = Math.abs(date2.getTime() - date1.getTime());
    return Math.ceil(timeDiff / (1000 * 3600 * 24));
  }
  
  test('formatDate() formats dates correctly', () => {
    const date = new Date('2023-06-15');
    
    expect(formatDate(date)).toBe('2023-06-15');
    expect(formatDate(date, 'DD/MM/YYYY')).toBe('15/06/2023');
    expect(formatDate(date, 'MM/DD/YYYY')).toBe('06/15/2023');
    
    // Edge cases
    expect(formatDate(null)).toBe('');
    expect(formatDate(undefined)).toBe('');
    expect(formatDate('not a date')).toBe('');
    expect(formatDate(new Date('invalid'))).toBe('');
  });
  
  test('isValidDate() validates dates correctly', () => {
    expect(isValidDate(new Date())).toBe(true);
    expect(isValidDate(new Date('2023-01-01'))).toBe(true);
    expect(isValidDate(new Date('invalid'))).toBe(false);
    expect(isValidDate(null)).toBe(false);
    expect(isValidDate(undefined)).toBe(false);
    expect(isValidDate('2023-01-01')).toBe(false);
    expect(isValidDate(123456789)).toBe(false);
  });
  
  test('daysBetween() calculates date differences', () => {
    const date1 = new Date('2023-01-01');
    const date2 = new Date('2023-01-04');
    const date3 = new Date('2023-01-01');
    
    expect(daysBetween(date1, date2)).toBe(3);
    expect(daysBetween(date2, date1)).toBe(3); // Should be absolute
    expect(daysBetween(date1, date3)).toBe(0);
    
    // Edge cases
    expect(daysBetween(null, date2)).toBe(0);
    expect(daysBetween(date1, 'invalid')).toBe(0);
    expect(daysBetween(new Date('invalid'), date2)).toBe(0);
  });
});

describe('Validation Utilities', () => {
  
  function isValidEmail(email) {
    if (typeof email !== 'string') return false;
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }
  
  function isValidUrl(url) {
    if (typeof url !== 'string') return false;
    try {
      new URL(url);
      return true;
    } catch {
      return false;
    }
  }
  
  function isNotEmpty(value) {
    if (value == null) return false;
    if (typeof value === 'string') return value.trim().length > 0;
    if (Array.isArray(value)) return value.length > 0;
    if (typeof value === 'object') return Object.keys(value).length > 0;
    return true;
  }
  
  test('isValidEmail() validates email addresses', () => {
    expect(isValidEmail('user@example.com')).toBe(true);
    expect(isValidEmail('test.user+tag@example.co.uk')).toBe(true);
    expect(isValidEmail('invalid.email')).toBe(false);
    expect(isValidEmail('@example.com')).toBe(false);
    expect(isValidEmail('user@')).toBe(false);
    expect(isValidEmail('')).toBe(false);
    expect(isValidEmail(null)).toBe(false);
    expect(isValidEmail(123)).toBe(false);
  });
  
  test('isValidUrl() validates URLs', () => {
    expect(isValidUrl('https://example.com')).toBe(true);
    expect(isValidUrl('http://localhost:3000')).toBe(true);
    expect(isValidUrl('ftp://files.example.com')).toBe(true);
    expect(isValidUrl('invalid-url')).toBe(false);
    expect(isValidUrl('http://')).toBe(false);
    expect(isValidUrl('')).toBe(false);
    expect(isValidUrl(null)).toBe(false);
    expect(isValidUrl(123)).toBe(false);
  });
  
  test('isNotEmpty() checks for empty values', () => {
    expect(isNotEmpty('hello')).toBe(true);
    expect(isNotEmpty([1, 2, 3])).toBe(true);
    expect(isNotEmpty({ key: 'value' })).toBe(true);
    expect(isNotEmpty(42)).toBe(true);
    expect(isNotEmpty(true)).toBe(true);
    
    expect(isNotEmpty('')).toBe(false);
    expect(isNotEmpty('   ')).toBe(false);
    expect(isNotEmpty([])).toBe(false);
    expect(isNotEmpty({})).toBe(false);
    expect(isNotEmpty(null)).toBe(false);
    expect(isNotEmpty(undefined)).toBe(false);
  });
}); 