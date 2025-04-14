import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from werkzeug.utils import secure_filename
import io # For sending file in memory
import sys # To flush print statements for logs

# --- Configuration ---
UPLOAD_FOLDER = 'uploads' # Optional: Store uploads temporarily
ALLOWED_EXTENSIONS = {'xlsx', 'csv'}
INCOME_RULES_FILE = 'income_rules.csv'
# Essential columns for basic operation and specific filtering logic
REQUIRED_COLUMNS = {
    'ACCOUNT NUMBER', 'TRANSACTION AMOUNT', 'TRANSACTION TYPE',
    'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2'
    # Add other columns from 7.1 if needed for display, though not for core logic
    # 'ACCOUNT NAME', 'TRANSACTION REF', etc.
}

app = Flask(__name__)
# Use environment variable in production, fallback for local dev
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER) # Create upload folder if it doesn't exist
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 70 * 1024 * 1024 # Allow ~70MB upload

# --- Helper Functions ---

def allowed_file(filename):
    """Checks if the file extension is allowed."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_income_rules(filepath=INCOME_RULES_FILE):
    """Loads income rules from CSV into a dictionary for quick lookup."""
    try:
        rules_df = pd.read_csv(filepath)
        # Convert ACCOUNT NUMBER to string for consistent matching
        rules_df['ACCOUNT NUMBER'] = rules_df['ACCOUNT NUMBER'].astype(str).str.strip() # Also strip whitespace here
        rules_df.set_index('ACCOUNT NUMBER', inplace=True)
        # Convert relevant columns to numeric, handling potential errors
        for col in ['FixedAmount', 'Percentage', 'Cap', 'ThresholdAmount']:
            if col in rules_df.columns:
                 rules_df[col] = pd.to_numeric(rules_df[col], errors='coerce')
        print(f"Successfully loaded {len(rules_df)} income rules.")
        sys.stdout.flush() # Ensure print appears in logs
        return rules_df.to_dict('index')
    except FileNotFoundError:
        print(f"ERROR: Income rules file '{filepath}' not found.")
        sys.stdout.flush()
        flash(f"Error: Income rules file '{filepath}' not found.", 'error')
        return None
    except Exception as e:
        print(f"ERROR loading income rules: {e}")
        sys.stdout.flush()
        flash(f"Error loading income rules: {e}", 'error')
        return None

def calculate_income(row, rules):
    """Calculates income for a single transaction row based on loaded rules."""
    # Ensure account number is string and stripped
    account_number = str(row.get('ACCOUNT NUMBER', '')).strip()
    amount = row.get('TRANSACTION AMOUNT') # Use .get for safety

    if not pd.notna(amount): # Handle missing transaction amounts
        return 0, None # Return 0 income and no warning message

    rule = rules.get(account_number)

    if rule:
        rule_type = rule.get('RuleType')
        try:
            if rule_type == 'Fixed':
                income = rule.get('FixedAmount', 0)
            elif rule_type == 'PercentageCap':
                percentage = rule.get('Percentage', 0)
                cap = rule.get('Cap')
                income = amount * percentage
                if pd.notna(cap) and income > cap:
                    income = cap
            elif rule_type == 'ConditionalFixed':
                threshold = rule.get('ThresholdAmount')
                fixed_amount = rule.get('FixedAmount', 0)
                if pd.notna(threshold) and amount > threshold:
                    income = fixed_amount
                else:
                    income = 0
            elif rule_type == 'None':
                income = 0
            else: # Default or unknown rule type
                income = 0
            # Ensure income is not NaN if calculations resulted in it
            return (income if pd.notna(income) else 0), None
        except Exception as e:
            # Log specific calculation error, default to 0 income
            warning = f"Calculation error for Acc {account_number}, Rule {rule_type}: {e}"
            print(f"WARNING: {warning}") # Log to console/server logs
            sys.stdout.flush()
            return 0, warning # Or return a specific error indicator if needed
    else:
        # Unmapped account number - FR4.2.5
        warning = f"Unmapped Account Number: {account_number}"
        # Avoid printing excessive warnings for the same unmapped account
        # Maybe collect unique unmapped accounts later if needed
        return 0, warning

def process_transactions(file_path, file_type, income_rules):
    """Reads, filters, calculates income, and aggregates transaction data."""
    print(f"--- Starting processing for file_type: {file_type} ---")
    sys.stdout.flush()
    try:
        if file_path.endswith('.xlsx'):
            print("Reading Excel file...")
            sys.stdout.flush()
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            print("Reading CSV file...")
            sys.stdout.flush()
            df = pd.read_csv(file_path)
        print("File read successfully.")
        sys.stdout.flush()
    except Exception as e:
        print(f"ERROR reading file: {e}")
        sys.stdout.flush()
        return None, f"Error reading file: {e}", None, None

    initial_row_count = len(df)
    print(f"Initial rows read: {initial_row_count}")
    if initial_row_count == 0:
        return pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total Transaction Volume', 'Total Transaction Value', 'Total Income']), \
               "Input file is empty.", \
               "Processed 0 rows.", \
               []

    # --- FR2: Data Parsing & Validation ---
    # Check for required columns (case-insensitive check)
    print("Checking required columns...")
    sys.stdout.flush()
    # Store original columns before uppercasing
    original_columns = list(df.columns)
    df.columns = [str(col).upper() for col in df.columns] # Ensure column names are strings before upper()
    current_upper_cols = set(df.columns)

    missing_cols = REQUIRED_COLUMNS - current_upper_cols
    if missing_cols:
         # Try matching original case-insensitively before failing hard
         col_mapping_lower = {col.lower(): col for col in original_columns}
         required_lower = {req.lower() for req in REQUIRED_COLUMNS}
         actual_missing = []
         for req_low in required_lower:
             if req_low not in col_mapping_lower:
                 # Find the original casing of the missing column name if possible
                 missing_name = next((rc for rc in REQUIRED_COLUMNS if rc.lower() == req_low), req_low)
                 actual_missing.append(missing_name)

         if actual_missing:
             err_msg = f"Missing required columns (case-insensitive check failed): {', '.join(actual_missing)}"
             print(f"ERROR: {err_msg}")
             sys.stdout.flush()
             return None, err_msg, None, None
         else:
              # This case shouldn't be logically reachable if REQUIRED_COLUMNS check passed initially,
              # but handles edge cases.
              print("INFO: All required columns found after case-insensitive mapping.")
              sys.stdout.flush()

    print("Required columns found.")
    sys.stdout.flush()

    # --- Data Cleaning & Preparation ---
    print("Cleaning data (types, whitespace)...")
    sys.stdout.flush()
    # Ensure essential numeric columns are numeric, coercing errors
    if 'TRANSACTION AMOUNT' in df.columns:
        df['TRANSACTION AMOUNT'] = pd.to_numeric(df['TRANSACTION AMOUNT'], errors='coerce')
        # Optionally handle rows with conversion errors (e.g., log, remove, or flag)
        rows_with_bad_amount = df['TRANSACTION AMOUNT'].isna().sum()
        if rows_with_bad_amount > 0:
            print(f"WARNING: {rows_with_bad_amount} rows had non-numeric TRANSACTION AMOUNT and will be excluded by amount filters.")
            sys.stdout.flush()
        # df.dropna(subset=['TRANSACTION AMOUNT'], inplace=True) # Example: remove rows with invalid amounts now

    # **Whitespace Trimming and Type Conversion for Filter Columns**
    str_cols_to_clean = ['TRANSACTION TYPE', 'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2', 'ACCOUNT NUMBER']
    for col in str_cols_to_clean:
        if col in df.columns:
            # Convert to string, strip whitespace, handle potential errors like NaN
            df[col] = df[col].astype(str).fillna('').str.strip()

    # Ensure uppercase for relevant columns AFTER stripping
    if 'TRANSACTION TYPE' in df.columns:
         df['TRANSACTION TYPE'] = df['TRANSACTION TYPE'].str.upper()
    if 'TRAN_PARTICULAR' in df.columns:
         df['TRAN_PARTICULAR'] = df['TRAN_PARTICULAR'].str.upper() # Already uppercased via column rename, but belt-and-suspenders

    print("Data cleaning finished.")
    # --- *** ADVANCED DEBUGGING: Print DataFrame Head *** ---
    print("--- DataFrame head after cleaning ---")
    try:
        print(df.head().to_string()) # .to_string() prevents truncation
    except Exception as head_err:
        print(f"Could not print df.head(): {head_err}")
    print("------------------------------------")
    sys.stdout.flush()
    # ----------------------------------------------------------


    # --- FR3: Transaction Filtering ---
    print("--- Applying Filters ---")
    sys.stdout.flush()

    # Define individual filters first for debugging
    try:
        f_type_is_c = (df['TRANSACTION TYPE'] == 'C')
        print(f"Rows where TRANSACTION TYPE == 'C': {f_type_is_c.sum()}")

        f_tp2_10_digits = df['TRAN_PARTICULAR_2'].str.match(r'^\d{10}')
        print(f"Rows where TRAN_PARTICULAR_2 starts with 10 digits: {f_tp2_10_digits.sum()}")

        f_not_rvsl = ~df['TRAN_PARTICULAR'].str.startswith('RVSL')
        print(f"Rows where TRAN_PARTICULAR does NOT start with RVSL: {f_not_rvsl.sum()}")

        f_not_reversal = ~df['TRAN_PARTICULAR'].str.startswith('REVERSAL')
        print(f"Rows where TRAN_PARTICULAR does NOT start with REVERSAL: {f_not_reversal.sum()}")

        f_amount_notna = df['TRANSACTION AMOUNT'].notna()
        print(f"Rows where TRANSACTION AMOUNT is not NaN: {f_amount_notna.sum()}")

        # Base filters (applied to most types)
        base_filter = ( f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal & f_amount_notna )
        print(f"Rows meeting ALL base filter conditions: {base_filter.sum()}")

        # Type-specific filters
        if file_type == 'Any Partner':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            print(f"Rows meeting specific [Any Partner]: Amount >= 100: {f_amount.sum()}")
            specific_filter = f_amount
        elif file_type == 'Fincra':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            f_not_name = (~df['TRAN_PARTICULAR'].str.startswith('NAME'))
            print(f"Rows meeting specific [Fincra]: Amount >= 100: {f_amount.sum()}")
            print(f"Rows meeting specific [Fincra]: TRAN_PARTICULAR does NOT start with NAME: {f_not_name.sum()}")
            specific_filter = (f_amount & f_not_name)
        elif file_type == 'Leatherback or Fusion':
            f_amount = (df['TRANSACTION AMOUNT'] >= 10000)
            print(f"Rows meeting specific [Leatherback/Fusion]: Amount >= 10000: {f_amount.sum()}")
            specific_filter = f_amount
        elif file_type == 'Cashout':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            f_not_ft = (~df['TRAN_PARTICULAR'].str.startswith('FT'))
            print(f"Rows meeting specific [Cashout]: Amount >= 100: {f_amount.sum()}")
            print(f"Rows meeting specific [Cashout]: TRAN_PARTICULAR does NOT start with FT: {f_not_ft.sum()}")
            specific_filter = (f_amount & f_not_ft)
        else:
            err_msg = f"Invalid file type selection: {file_type}"
            print(f"ERROR: {err_msg}")
            sys.stdout.flush()
            return None, err_msg, None, None

        # Combine base and specific filters
        combined_filter = base_filter & specific_filter
        print(f"Rows meeting ALL base AND specific conditions for '{file_type}': {combined_filter.sum()}")
        sys.stdout.flush()

        filtered_df = df[combined_filter].copy() # Use .copy() to avoid SettingWithCopyWarning

    except KeyError as ke:
        # Catch errors if a column name used in filtering doesn't exist after cleaning/uppercasing
        err_msg = f"Filtering error: Column not found - {ke}. Check input file structure."
        print(f"ERROR: {err_msg}")
        sys.stdout.flush()
        return None, err_msg, None, None
    except Exception as filter_err:
        # Catch other potential errors during filtering
        err_msg = f"An unexpected error occurred during filtering: {filter_err}"
        print(f"ERROR: {err_msg}")
        sys.stdout.flush()
        return None, err_msg, None, None

    filtered_row_count = len(filtered_df)
    print(f"--- Filtering Complete ---")
    print(f"Rows remaining after combined filter: {filtered_row_count}")
    sys.stdout.flush()

    if filtered_df.empty:
        print("No transactions passed the filtering criteria.")
        sys.stdout.flush()
        return pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total Transaction Volume', 'Total Transaction Value', 'Total Income']), \
               "No transactions passed the filtering criteria.", \
               f"Processed {initial_row_count} rows, Filtered down to 0 rows.", \
               [] # No warnings if no rows to process

    # --- FR4: Per-Transaction Income Calculation ---
    print("Calculating income per transaction...")
    sys.stdout.flush()
    warnings = []
    # Use a list comprehension for potentially better performance than apply lambda
    # and easier handling of multiple return values
    income_results_list = []
    for index, row in filtered_df.iterrows():
        income, warning = calculate_income(row, income_rules)
        income_results_list.append(income)
        if warning:
            warnings.append(warning)

    filtered_df['Income'] = income_results_list
    unique_warnings = sorted(list(set(warnings))) # Get unique warnings
    print(f"Income calculation complete. Found {len(unique_warnings)} unique warnings during calculation.")
    sys.stdout.flush()


    # --- FR4.4 & FR4.5: Aggregation by Partner (ACCOUNT NUMBER) ---
    print("Aggregating results by ACCOUNT NUMBER...")
    sys.stdout.flush()
    # Ensure ACCOUNT NUMBER is string for consistent grouping (already done in cleaning)
    # filtered_df['ACCOUNT NUMBER'] = filtered_df['ACCOUNT NUMBER'].astype(str)

    aggregated_results = filtered_df.groupby('ACCOUNT NUMBER').agg(
        # Use a column that exists for size, like TRANSACTION_AMOUNT or rely on index if needed
        Total_Transaction_Volume=('TRANSACTION AMOUNT', 'size'), # Count rows in each group
        Total_Transaction_Value=('TRANSACTION AMOUNT', 'sum'),
        Total_Income=('Income', 'sum')
    ).reset_index() # Convert grouped output back to DataFrame
    print("Aggregation complete.")
    sys.stdout.flush()

    # --- Prepare Summary ---
    summary_stats = f"Processed {initial_row_count} rows. Filtered down to {filtered_row_count} relevant transactions. Found {len(aggregated_results)} unique partners."
    if unique_warnings:
        # Limit number of warnings displayed in summary for brevity
        max_warnings_in_summary = 5
        display_warnings = unique_warnings[:max_warnings_in_summary]
        warning_summary = "<br>Warnings encountered (showing first {}):<ul>".format(len(display_warnings)) + "".join(f"<li>{w}</li>" for w in display_warnings) + "</ul>"
        if len(unique_warnings) > max_warnings_in_summary:
            warning_summary += f"<i>...and {len(unique_warnings) - max_warnings_in_summary} more unique warnings (check logs).</i>"
        summary_stats += warning_summary
        # Also flash unique warnings for visibility if desired (can be noisy)
        # for warn in unique_warnings:
        #     flash(warn, 'warning')

    print("--- Processing Finished ---")
    sys.stdout.flush()
    return aggregated_results, None, summary_stats, unique_warnings # Return results, no error, summary, warnings

# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
    # Clear previous results from session on new visit
    session.pop('results_df_json', None)
    session.pop('summary_stats', None)
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file upload, processing, and renders results."""
    if 'file' not in request.files:
        flash('No file part', 'error')
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No selected file', 'error')
        return redirect(request.url)

    file_type = request.form.get('file_type')
    if not file_type:
        flash('Please select a Partner/Filter Type', 'error')
        return redirect(request.url)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        # Define file_path within try block scope if saving
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        saved_file = False # Flag to track if file was saved

        try:
            print(f"Attempting to save uploaded file: {filename}")
            sys.stdout.flush()
            file.save(file_path)
            saved_file = True
            print(f"File saved successfully to {file_path}")
            flash('File uploaded successfully. Processing...', 'success')
            sys.stdout.flush()


            # Load income rules
            print("Loading income rules...")
            sys.stdout.flush()
            income_rules = load_income_rules()
            if income_rules is None:
                 # Error flashed in load_income_rules
                 # No need to remove file here, finally block handles it
                 return redirect(url_for('index'))

            # Process the file
            print("Calling process_transactions...")
            sys.stdout.flush()
            results_df, error_msg, summary, warnings = process_transactions(file_path, file_type, income_rules)
            print("process_transactions finished.")
            sys.stdout.flush()


            if error_msg:
                flash(error_msg, 'error')
                 # No need to remove file here, finally block handles it
                return redirect(url_for('index'))

            # Store results in session for download
            session['results_df_json'] = results_df.to_json(orient='split', date_format='iso')
            session['summary_stats'] = summary

            print("Rendering results page.")
            sys.stdout.flush()
            # Render results on the page
            return render_template('index.html',
                                   results=results_df.to_dict('records'),
                                   summary_stats=summary)

        except Exception as e:
             print(f"ERROR in upload_file route: {e}")
             import traceback
             print(traceback.format_exc()) # Print full traceback to logs
             sys.stdout.flush()
             flash(f"An unexpected error occurred during processing: {e}", 'error')
             return redirect(url_for('index'))
        finally:
            # Clean up uploaded file if it was saved
            if saved_file and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"Cleaned up temporary file: {file_path}")
                    sys.stdout.flush()
                except Exception as cleanup_err:
                     print(f"WARNING: Could not remove temporary file {file_path}: {cleanup_err}")
                     sys.stdout.flush()

    else:
        flash('Invalid file type. Allowed types: .xlsx, .csv', 'error')
        return redirect(request.url)

@app.route('/download', methods=['POST'])
def download_results():
    """Handles downloading the processed results stored in session."""
    results_json = session.get('results_df_json')
    if not results_json:
        flash('No results available to download.', 'error')
        return redirect(url_for('index'))

    try:
        results_df = pd.read_json(results_json, orient='split')

        # Create an in-memory Excel file
        output = io.BytesIO()
        # Use context manager for ExcelWriter
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            results_df.to_excel(writer, index=False, sheet_name='Analysis Results')
        # No need to writer.save() with context manager
        output.seek(0) # Rewind the buffer

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='transaction_analysis_results.xlsx' # Use download_name
        )
    except Exception as e:
        print(f"ERROR generating download file: {e}")
        import traceback
        print(traceback.format_exc())
        sys.stdout.flush()
        flash(f"Error generating download file: {e}", 'error')
        return redirect(url_for('index'))


# --- Run Application ---
if __name__ == '__main__':
    # Set host='0.0.0.0' to be accessible externally if running locally without Gunicorn
    # Port 5000 is the default, change if needed
    # Use debug=True only for local development, Gunicorn handles production.
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
    # Note: When deploying with Gunicorn, Gunicorn starts the app,
    # so this __main__ block isn't executed on the server.
    # Gunicorn command: gunicorn --timeout 120 app:app --bind 0.0.0.0:$PORT
