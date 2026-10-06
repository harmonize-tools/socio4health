# Troubleshooting Guide: Harmonizer Errors

## Error: KeyError: 'category' in s4h_data_selector

### Problem
When calling `har.s4h_data_selector(dfs)`, you get this error:
```
KeyError: "Column 'category' not found in dictionary. Ensure the dictionary has been classified using s4h_classify_rows(). Available columns: [...]"
```

### Root Cause
The `s4h_data_selector()` method requires that the dictionary DataFrame has been classified using `s4h_classify_rows()` to add the `'category'` column. If this step is missing, the method cannot filter by categories.

### Solution
Before calling `s4h_data_selector()`, ensure you have:

1. **Standardized the dictionary** using `harmonizer_utils.s4h_standardize_dict()`
2. **Translated the columns** to English using `harmonizer_utils.s4h_translate_column()`
3. **Classified the rows** using `harmonizer_utils.s4h_classify_rows()` to add the `'category'` column
4. **Set the dictionary** to the Harmonizer instance using `har.dict_df = dic`

### Example Usage

```python
from socio4health.utils import harmonizer_utils
from socio4health.harmonizer import Harmonizer
import pandas as pd

# Step 1: Load and standardize the dictionary
raw_dic = pd.read_excel("raw_dictionary_br_2010.xlsx")
dic = harmonizer_utils.s4h_standardize_dict(raw_dic)

# Step 2: Translate to English
dic = harmonizer_utils.s4h_translate_column(dic, "question", language="en")
dic = harmonizer_utils.s4h_translate_column(dic, "description", language="en")
dic = harmonizer_utils.s4h_translate_column(dic, "possible_answers", language="en")

# Step 3: Classify rows (THIS IS IMPORTANT!)
dic = harmonizer_utils.s4h_classify_rows(
    dic, 
    "question_en", 
    "description_en", 
    "possible_answers_en",
    new_column_name="category",
    MODEL_PATH="dirreno/harmonize_bert_finetuned_classifier"
)

# Step 4: Create Harmonizer and set dictionary
har = Harmonizer()
har.dict_df = dic  # Set the CLASSIFIED dictionary
har.categories = ["Business", "Housing"]  # Your categories of interest
har.key_col = 'V0001'

# Step 5: Now you can use s4h_data_selector
filtered_dfs = har.s4h_data_selector(dfs)
```

### Verification

Before calling `s4h_data_selector()`, verify that the `'category'` column exists:

```python
# Check if 'category' column exists
if 'category' in dic.columns:
    print("✓ Dictionary has been classified correctly")
    print("Available categories:", dic['category'].unique())
else:
    print("✗ Dictionary is missing the 'category' column")
    print("Available columns:", dic.columns.tolist())
```

## Error: No rows found matching key values in DataFrame

### Problem
When calling `s4h_data_selector()`, you get a warning:
```
WARNING - No rows found matching key values in DataFrame
```

### Root Cause
The `key_col` value(s) specified in `har.key_val` don't match any rows in the DataFrame.

### Solution
Ensure that:
1. The `key_col` exists in the DataFrame
2. The `key_val` values exist in that column
3. The column names are uppercase (they will be automatically converted)

```python
har = Harmonizer()
har.key_col = 'V0001'  # The key column name
har.key_val = ['11', '12', '13']  # States Rondônia, Acre, Amazonas

# Verify before calling s4h_data_selector
print("Key column:", har.key_col)
print("Key values:", har.key_val)

filtered_dfs = har.s4h_data_selector(dfs)
```

## Error: No columns found matching the specified categories

### Problem
You get this warning:
```
WARNING - No columns found matching the specified categories. Use compare_with_dict() for more information.
```

### Root Cause
No variables in the dictionary have the specified categories, or there's a mismatch between the categories and the dictionary.

### Solution
1. Check what categories are available in the dictionary:
```python
print("Available categories:", dic['category'].unique())
```

2. Use valid category names:
```python
har.categories = ["Business", "Housing"]  # Use correct category names
```

3. Use `compare_with_dict()` to see more details:
```python
har.compare_with_dict(dfs)
```
