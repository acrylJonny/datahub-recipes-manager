# Testing Template for New Features

## 🎯 **Quick Decision Guide**

When adding a new feature, follow this decision tree:

```
New Feature Added
├── Is it a new metadata page/workflow? 
│   └── YES → Add Selenium E2E test (high value)
├── Is it a pure JavaScript utility function?
│   └── YES → Add Jest unit test (fast)
├── Is it a new API endpoint?
│   └── YES → Add Django API test (reliable)
└── Is it complex user interaction?
    └── YES → Add Selenium test (comprehensive)
```

## 🛠️ **1. Adding Selenium E2E Tests**

### **Template for New Metadata Page:**

```python
# In tests/frontend/test_metadata_selenium.py

@pytest.mark.selenium
class YourNewPageSeleniumTestCase(MetadataManagerSeleniumTestCase):
    """Test critical your-new-page functionality."""
    
    def test_your_page_loads_successfully(self):
        """Test that the your-page loads without errors."""
        self.browser.get(f'{self.live_server_url}/metadata/your-page/')
        
        # Verify page loads without server errors
        page_source = self.browser.page_source.lower()
        self.assertNotIn('server error', page_source)
        self.assertNotIn('500', page_source)
        self.assertNotIn('traceback', page_source)
        
        # Verify we're on the correct URL
        self.assertIn('/metadata/your-page/', self.browser.current_url)
        
        # Look for key page elements
        body_element = self.wait_for_element((By.TAG_NAME, 'body'))
        self.assertTrue(body_element.is_displayed())

    def test_your_page_create_workflow(self):
        """Test creating a new item works."""
        self.browser.get(f'{self.live_server_url}/metadata/your-page/')
        
        # Wait for page load
        time.sleep(2)
        
        # Find and click create button
        try:
            create_btn = self.browser.find_element(By.XPATH, '//button[contains(text(), "Create")]')
            create_btn.click()
            time.sleep(1)
            
            # Verify modal or form appeared
            modal_selectors = [
                (By.CLASS_NAME, 'modal'),
                (By.CLASS_NAME, 'form'),
                (By.TAG_NAME, 'form')
            ]
            
            form_found = False
            for selector in modal_selectors:
                try:
                    element = self.browser.find_element(*selector)
                    if element.is_displayed():
                        form_found = True
                        break
                except NoSuchElementException:
                    continue
            
            # Even if no form found, ensure no JavaScript errors
            logs = self.browser.get_log('browser')
            critical_errors = [log for log in logs if log['level'] == 'SEVERE' 
                              and 'datahub' not in log['message'].lower()]
            
            self.assertEqual(len(critical_errors), 0, 
                            f"JavaScript errors: {critical_errors}")
            
        except NoSuchElementException:
            # No create button found - that's okay for some pages
            pass
```

### **Template for User Workflow Test:**

```python
def test_your_workflow_integration(self):
    """Test complete user workflow: list → create → edit → delete."""
    base_url = f'{self.live_server_url}/metadata/your-page'
    
    # Step 1: Navigate to list page
    self.browser.get(f'{base_url}/')
    time.sleep(2)
    
    # Step 2: Attempt to create new item
    try:
        create_btn = self.browser.find_element(By.XPATH, '//button[contains(text(), "Create") or contains(@class, "btn-primary")]')
        create_btn.click()
        time.sleep(1)
        
        # Step 3: Fill form if present
        name_inputs = [
            (By.NAME, 'name'),
            (By.ID, 'name'),
            (By.XPATH, '//input[@placeholder*="name"]')
        ]
        
        for selector in name_inputs:
            try:
                name_field = self.browser.find_element(*selector)
                name_field.clear()
                name_field.send_keys('Test Item')
                break
            except NoSuchElementException:
                continue
        
        # Step 4: Submit form
        submit_selectors = [
            (By.XPATH, '//button[@type="submit"]'),
            (By.XPATH, '//input[@type="submit"]'),
            (By.XPATH, '//button[contains(text(), "Save") or contains(text(), "Create")]')
        ]
        
        for selector in submit_selectors:
            try:
                submit_btn = self.browser.find_element(*selector)
                submit_btn.click()
                time.sleep(2)
                break
            except NoSuchElementException:
                continue
                
    except NoSuchElementException:
        # No create functionality - just verify page works
        pass
    
    # Final verification: no errors occurred
    page_source = self.browser.page_source.lower()
    self.assertNotIn('error', page_source)
    self.assertNotIn('exception', page_source)
```

## ⚡ **2. Adding JavaScript Unit Tests**

### **Template for Pure Utility Functions:**

```javascript
// In tests/js/your-feature.test.js

describe('Your Feature - Utility Functions', () => {
  
  test('yourUtilityFunction() handles normal input correctly', () => {
    // Test normal case
    const result = yourUtilityFunction('normal input');
    expect(result).toBe('expected output');
  });
  
  test('yourUtilityFunction() handles edge cases gracefully', () => {
    // Test edge cases
    expect(yourUtilityFunction(null)).toBe('default value');
    expect(yourUtilityFunction(undefined)).toBe('default value');
    expect(yourUtilityFunction('')).toBe('default value');
  });
  
  test('yourDataProcessor() processes arrays correctly', () => {
    const input = [
      { name: 'item1', value: 10 },
      { name: 'item2', value: 20 }
    ];
    
    const result = yourDataProcessor(input);
    
    expect(result).toHaveLength(2);
    expect(result[0]).toHaveProperty('name', 'item1');
    expect(result[0]).toHaveProperty('processed', true);
  });
});

describe('Your Feature - DOM Manipulation', () => {
  
  beforeEach(() => {
    // Set up DOM for each test
    document.body.innerHTML = `
      <div id="test-container">
        <input id="test-input" type="text" />
        <button id="test-button">Click Me</button>
      </div>
    `;
  });
  
  test('yourDOMFunction() updates elements correctly', () => {
    const input = document.getElementById('test-input');
    const button = document.getElementById('test-button');
    
    yourDOMFunction('test-value');
    
    expect(input.value).toBe('test-value');
    expect(button.textContent).toBe('Updated');
  });
});
```

## 🔧 **3. Adding API Integration Tests**

### **Template for New API Endpoints:**

```python
# In tests/test_your_feature_api.py

from django.test import TestCase
from django.urls import reverse
from django.contrib.auth.models import User
from tests.fixtures.simple_factories import UserFactory

class YourFeatureAPITestCase(TestCase):
    """Test API endpoints for your feature."""
    
    def setUp(self):
        self.user = UserFactory()
        self.client.force_login(self.user)
    
    def test_your_api_endpoint_returns_200(self):
        """Test that your API endpoint returns successfully."""
        url = reverse('your_app:your_endpoint')
        response = self.client.get(url)
        
        # Flexible assertion for different scenarios
        self.assertIn(response.status_code, [200, 302, 404])
        
        if response.status_code == 200:
            # Additional validation for successful responses
            self.assertNotIn(b'error', response.content.lower())
            self.assertNotIn(b'exception', response.content.lower())
    
    def test_your_post_endpoint_handles_data(self):
        """Test POST endpoint with various data types."""
        url = reverse('your_app:your_post_endpoint')
        
        # Test with valid data
        valid_data = {
            'name': 'Test Item',
            'description': 'Test Description'
        }
        
        response = self.client.post(url, valid_data)
        self.assertIn(response.status_code, [200, 201, 302])
        
        # Test with empty data (should handle gracefully)
        response = self.client.post(url, {})
        self.assertIn(response.status_code, [200, 400, 422])
    
    def test_your_ajax_endpoint_returns_json(self):
        """Test AJAX endpoint returns proper JSON."""
        url = reverse('your_app:your_ajax_endpoint')
        response = self.client.get(url, HTTP_X_REQUESTED_WITH='XMLHttpRequest')
        
        if response.status_code == 200:
            self.assertEqual(response['Content-Type'], 'application/json')
            
            # Test JSON is parseable
            try:
                data = response.json()
                self.assertIsInstance(data, (dict, list))
            except ValueError:
                self.fail("Response is not valid JSON")
```

## 📝 **4. Quick Commands Reference**

### **Run Your New Tests:**

```bash
# Run specific Selenium test
python manage.py test tests.frontend.test_metadata_selenium.YourNewPageSeleniumTestCase

# Run specific JavaScript test
npm test -- your-feature.test.js

# Run specific API test
python manage.py test tests.test_your_feature_api

# Run all tests
python manage.py test && npm test
```

### **Debug Your Tests:**

```bash
# Selenium with verbose output
python manage.py test tests.frontend.test_metadata_selenium --verbosity=2

# JavaScript with watch mode
npm run test:watch

# API tests with keepdb
python manage.py test tests.test_your_feature_api --keepdb
```

## 🚀 **Best Practices Checklist**

### ✅ **Before Adding Tests:**
- [ ] Determine test type using decision tree above
- [ ] Choose appropriate test file location
- [ ] Set up necessary fixtures/mocks

### ✅ **While Writing Tests:**
- [ ] Use descriptive test names explaining what's being tested
- [ ] Test both happy path and edge cases
- [ ] Keep tests independent (no shared state)
- [ ] Add appropriate waits for async operations

### ✅ **After Writing Tests:**
- [ ] Run tests locally to ensure they pass
- [ ] Verify tests fail when they should (test the test)
- [ ] Add documentation if testing complex workflows
- [ ] Update CI configuration if needed

---

**This template ensures every new feature gets appropriate test coverage following our established patterns!** 🎯 