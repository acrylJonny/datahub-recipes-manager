/**
 * Jest setup file for JavaScript unit tests
 * Configures testing environment and provides common utilities
 */

require('@testing-library/jest-dom');

// Mock jQuery since our code uses it
global.$ = global.jQuery = {
  ajax: jest.fn(),
  get: jest.fn(),
  post: jest.fn(),
  ready: jest.fn(callback => callback()),
  on: jest.fn(),
  click: jest.fn(),
  val: jest.fn(),
  text: jest.fn(),
  html: jest.fn(),
  show: jest.fn(),
  hide: jest.fn(),
  attr: jest.fn(),
  prop: jest.fn(),
  addClass: jest.fn(),
  removeClass: jest.fn(),
  hasClass: jest.fn(),
  find: jest.fn(() => ({
    length: 0,
    show: jest.fn(),
    hide: jest.fn(),
    text: jest.fn(),
    html: jest.fn(),
    val: jest.fn(),
    attr: jest.fn(),
    prop: jest.fn()
  })),
  each: jest.fn()
};

// Mock Django CSRF token
global.getCookie = jest.fn(() => 'mock-csrf-token');

// Mock console methods to avoid noise in tests
global.console = {
  ...console,
  log: jest.fn(),
  error: jest.fn(),
  warn: jest.fn(),
  info: jest.fn()
};

// Mock localStorage
const localStorageMock = {
  getItem: jest.fn(),
  setItem: jest.fn(),
  removeItem: jest.fn(),
  clear: jest.fn(),
};
global.localStorage = localStorageMock;

// Mock fetch API
global.fetch = jest.fn();

// Reset all mocks before each test
beforeEach(() => {
  jest.clearAllMocks();
  fetch.mockClear();
  localStorageMock.getItem.mockClear();
  localStorageMock.setItem.mockClear();
}); 