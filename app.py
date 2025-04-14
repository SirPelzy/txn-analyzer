import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from werkzeug.utils import secure_filename
import io # For sending file in memory
import sys # To flush print statements for logs
import traceback # For detailed error logging

# --- Configuration ---
UPLOAD_FOLDER = 'uploads' # Optional: Store uploads temporarily
ALLOWED_EXTENSIONS = {'xlsx', 'csv'}
INCOME_RULES_FILE = 'income_rules.csv'
# Essential columns for basic operation and specific filtering logic
REQUIRED_COLUMNS = {
    'ACCOUNT NUMBER', 'TRANSACTION AMOUNT', 'PART TRAN TYPE',
    'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2'
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
        rules_df['ACCOUNT NUMBER'] = rules_df['ACCOUNT NUMBER'].astype(str).str.strip()
        rules_df.set_index('ACCOUNT NUMBER', inplace=True)
        for col in ['FixedAmount', 'Percentage', 'Cap', 'ThresholdAmount']:
            if col in rules_df.columns:
                 rules_df[col] = pd.to_numeric(rules_df[col], errors='coerce')
        print(f"Successfully loaded {len(rules_df)} income rules.")
        sys.stdout.flush()
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
    account_number = str(row.get('ACCOUNT NUMBER', '')).strip()
    amount = row.get('TRANSACTION AMOUNT')

    # --- Optional Debug Print inside calculate_income ---
    # if account_number == '3000002151': # Or any specific account number you want to trace
    #     print(f"Calculating income for {account_number}, Amount: {amount}")
    #     sys.stdout.flush()
    # --- End Debug Print ---

    if not pd.notna(amount):
        return 0, None

    rule = rules.get(account_number)

    # --- Optional Debug Print inside calculate_income ---
    # if account_number == '3000002151':
    #     print(f"  Rule found for {account_number}: {rule}")
    #     sys.stdout.flush()
    # --- End Debug Print ---

    if rule:
        rule_type = rule.get('RuleType')
        try:
            income = 0 # Default income if no rule matches below
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
                    income = 0 # Explicitly 0 if condition not met
            elif rule_type == 'None':
                income = 0

            # Ensure income is not NaN if calculations resulted in it
            final_income = income if pd.notna(income) else 0

            # --- Optional Debug Print inside calculate_income ---
            # if account_number == '3000002151':
            #     print(f"  Calculated income for {account_number}: {final_income}")
            #     sys.stdout.flush()
            # --- End Debug Print ---

            return final_income, None
        except Exception as e:
            warning = f"Calculation error for Acc {account_number}, Rule {rule_type}: {e}"
            print(f"WARNING: {warning}")
            sys.stdout.flush()
            return 0, warning
    else:
        warning = f"Unmapped Account Number: {account_number}"
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
            df = pd.read_csv(file_path, low_memory=False) # Added low_memory=False for potential mixed types
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
    print("Checking required columns...")
    sys.stdout.flush()
    original_columns = list(df.columns)
    df.columns = [str(col).upper().strip() for col in df.columns] # Add strip() to column names
    current_upper_cols = set(df.columns)

    missing_cols = REQUIRED_COLUMNS - current_upper_cols
    if missing_cols:
         col_mapping_lower = {str(col).lower(): col for col in original_columns}
         required_lower = {req.lower() for req in REQUIRED_COLUMNS}
         actual_missing = []
         for req_low in required_lower:
             if req_low not in col_mapping_lower:
                 missing_name = next((rc for rc in REQUIRED_COLUMNS if rc.lower() == req_low), req_low)
                 actual_missing.append(missing_name)
         if actual_missing:
             err_msg = f"Missing required columns (case-insensitive check failed): {', '.join(actual_missing)}"
             print(f"ERROR: {err_msg}")
             sys.stdout.flush()
             return None, err_msg, None, None

    print("Required columns found.")
    sys.stdout.flush()

    # --- Data Cleaning & Preparation ---
    print("Cleaning data (types, whitespace)...")
    sys.stdout.flush()
    if 'TRANSACTION AMOUNT' in df.columns:
        df['TRANSACTION AMOUNT'] = pd.to_numeric(df['TRANSACTION AMOUNT'], errors='coerce')
        rows_with_bad_amount = df['TRANSACTION AMOUNT'].isna().sum()
        if rows_with_bad_amount > 0:
            print(f"INFO: {rows_with_bad_amount} rows had non-numeric TRANSACTION AMOUNT.")
            sys.stdout.flush()

    str_cols_to_clean = ['PART TRAN TYPE', 'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2', 'ACCOUNT NUMBER']
    for col in str_cols_to_clean:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('').str.strip()

    if 'PART TRAN TYPE' in df.columns:
         df['PART TRAN TYPE'] = df['PART TRAN TYPE'].str.upper()
    if 'TRAN_PARTICULAR' in df.columns:
         df['TRAN_PARTICULAR'] = df['TRAN_PARTICULAR'].str.upper()

    print("Data cleaning finished.")
    print("--- DataFrame head after cleaning (Sample) ---")
    try:
        print(df.head().to_string())
    except Exception as head_err:
        print(f"Could not print df.head(): {head_err}")
    print("------------------------------------")
    sys.stdout.flush()

    # --- FR3: Transaction Filtering ---
    print("--- Applying Filters ---")
    sys.stdout.flush()
    try:
        # Define individual filters first for debugging
        f_type_is_c = (df['PART TRAN TYPE'] == 'C')
        print(f"Rows where PART TRAN TYPE == 'C': {f_type_is_c.sum()}")

        # Using regex match requires non-NA values, fillna done above
        f_tp2_10_digits = df['TRAN_PARTICULAR_2'].str.match(r'^\d{10}')
        print(f"Rows where TRAN_PARTICULAR_2 starts with 10 digits: {f_tp2_10_digits.sum()}")

        f_not_rvsl = ~df['TRAN_PARTICULAR'].str.startswith('RVSL')
        print(f"Rows where TRAN_PARTICULAR does NOT start with RVSL: {f_not_rvsl.sum()}")

        f_not_reversal = ~df['TRAN_PARTICULAR'].str.startswith('REVERSAL')
        print(f"Rows where TRAN_PARTICULAR does NOT start with REVERSAL: {f_not_reversal.sum()}")

        # Ensure we only consider rows where amount is valid for amount-based filters
        f_amount_notna = df['TRANSACTION AMOUNT'].notna()
        print(f"Rows where TRANSACTION AMOUNT is not NaN: {f_amount_notna.sum()}")

        base_filter = ( f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal & f_amount_notna )
        print(f"Rows meeting ALL base filter conditions: {base_filter.sum()}")

        # Type-specific filters (ensure amount comparisons happen on non-NaN values)
        if file_type == 'Any Partner':
            f_amount = (df.loc[f_amount_notna, 'TRANSACTION AMOUNT'] >= 100)
            print(f"Rows meeting specific [Any Partner]: Amount >= 100 (among non-NaN amounts): {f_amount.sum()}")
            specific_filter = (df['TRANSACTION AMOUNT'] >= 100) # Apply on original index alignment
        elif file_type == 'Fincra':
            f_amount = (df.loc[f_amount_notna, 'TRANSACTION AMOUNT'] >= 100)
            f_not_name = (~df['TRAN_PARTICULAR'].str.startswith('NAME'))
            print(f"Rows meeting specific [Fincra]: Amount >= 100 (among non-NaN amounts): {f_amount.sum()}")
            print(f"Rows meeting specific [Fincra]: TRAN_PARTICULAR does NOT start with NAME: {f_not_name.sum()}")
            specific_filter = ((df['TRANSACTION AMOUNT'] >= 100) & f_not_name)
        elif file_type == 'Leatherback or Fusion':
            f_amount = (df.loc[f_amount_notna, 'TRANSACTION AMOUNT'] >= 10000)
            print(f"Rows meeting specific [Leatherback/Fusion]: Amount >= 10000 (among non-NaN amounts): {f_amount.sum()}")
            specific_filter = (df['TRANSACTION AMOUNT'] >= 10000)
        elif file_type == 'Cashout':
            f_amount = (df.loc[f_amount_notna, 'TRANSACTION AMOUNT'] >= 100)
            f_not_ft = (~df['TRAN_PARTICULAR'].str.startswith('FT'))
            print(f"Rows meeting specific [Cashout]: Amount >= 100 (among non-NaN amounts): {f_amount.sum()}")
            print(f"Rows meeting specific [Cashout]: TRAN_PARTICULAR does NOT start with FT: {f_not_ft.sum()}")
            specific_filter = ((df['TRANSACTION AMOUNT'] >= 100) & f_not_ft)
        else:
            err_msg = f"Invalid file type selection: {file_type}"
            print(f"ERROR: {err_msg}")
            sys.stdout.flush()
            return None, err_msg, None, None

        combined_filter = base_filter & specific_filter
        # Ensure NaN amounts don't accidentally pass the specific filter if >= comparison returns False
        combined_filter = combined_filter & f_amount_notna

        print(f"Rows meeting ALL base AND specific conditions for '{file_type}': {combined_filter.sum()}")
        sys.stdout.flush()

        filtered_df = df[combined_filter].copy()

    except KeyError as ke:
        err_msg = f"Filtering error: Column not found - {ke}. Check input file structure."
        print(f"ERROR: {err_msg}")
        sys.stdout.flush()
        return None, err_msg, None, None
    except Exception as filter_err:
        err_msg = f"An unexpected error occurred during filtering: {filter_err}"
        print(f"ERROR: {err_msg} \n{traceback.format_exc()}")
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
               []

    # --- FR4: Per-Transaction Income Calculation ---
    print("Calculating income per transaction...")
    sys.stdout.flush()
    warnings = []
    income_results_list = []
    # Check if income_rules were loaded
    if income_rules is None:
         print("ERROR: Income rules not loaded, cannot calculate income.")
         sys.stdout.flush()
         # Return zero income or handle as appropriate
         filtered_df['Income'] = 0 # Set all income to 0
    else:
        for index, row in filtered_df.iterrows():
            income, warning = calculate_income(row, income_rules)
            income_results_list.append(income)
            if warning:
                warnings.append(warning)
        filtered_df['Income'] = income_results_list

    unique_warnings = sorted(list(set(warnings)))
    print(f"Income calculation complete. Found {len(unique_warnings)} unique warnings during calculation.")
    sys.stdout.flush()


    # --- FR4.4 & FR4.5: Aggregation by Partner (ACCOUNT NUMBER) ---
    # --- *** DETAILED DEBUGGING BEFORE AGGREGATION *** ---
    print(f"\n--- DataFrame BEFORE Aggregation (Account: {filtered_df['ACCOUNT NUMBER'].unique()}) ---")
    print(f"Shape of filtered_df: {filtered_df.shape}")
    print(f"Data types of relevant columns:\n{filtered_df[['ACCOUNT NUMBER', 'TRANSACTION AMOUNT', 'Income']].dtypes}")
    print(f"\nInfo for filtered_df:")
    filtered_df.info() # Provides non-null counts and dtypes
    print(f"\nChecking for NaN values:")
    print(f"  NaN in ACCOUNT NUMBER: {filtered_df['ACCOUNT NUMBER'].isna().sum()}")
    print(f"  NaN in TRANSACTION AMOUNT: {filtered_df['TRANSACTION AMOUNT'].isna().sum()}")
    print(f"  NaN in Income: {filtered_df['Income'].isna().sum()}")

    print(f"\nChecking data for group '3000002151' (if exists):")
    # Ensure we only try this if the account number actually exists in the filtered data
    unique_accounts = filtered_df['ACCOUNT NUMBER'].unique()
    if '3000002151' in unique_accounts:
        acc_3000_rows = filtered_df[filtered_df['ACCOUNT NUMBER'] == '3000002151']
        print(f"  Number of rows for 3000002151: {len(acc_3000_rows)}")
        if not acc_3000_rows.empty:
            print(f"  Sample TRANSACTION AMOUNTs:\n{acc_3000_rows['TRANSACTION AMOUNT'].head()}")
            print(f"  Is TRANSACTION AMOUNT numeric? {pd.api.types.is_numeric_dtype(acc_3000_rows['TRANSACTION AMOUNT'])}")
            # Calculate sum directly on the subset for verification
            try:
                 manual_sum_amount = acc_3000_rows['TRANSACTION AMOUNT'].sum()
                 print(f"  Direct sum of TRANSACTION AMOUNT for 3000002151: {manual_sum_amount}")
            except Exception as sum_err:
                 print(f"  Could not calculate direct sum of TRANSACTION AMOUNT: {sum_err}")

            print(f"  Sample Income values:\n{acc_3000_rows['Income'].head()}")
            print(f"  Is Income numeric? {pd.api.types.is_numeric_dtype(acc_3000_rows['Income'])}")
            # Calculate sum directly on the subset for verification
            try:
                manual_sum_income = acc_3000_rows['Income'].sum()
                print(f"  Direct sum of Income for 3000002151: {manual_sum_income}")
            except Exception as sum_err:
                 print(f"  Could not calculate direct sum of Income: {sum_err}")
    else:
         print("  Account '3000002151' not found in filtered data (this shouldn't happen based on previous logs).")
    print("--------------------------------------------")
    sys.stdout.flush()
    # --- *** END DETAILED DEBUGGING *** ---

    print("Aggregating results by ACCOUNT NUMBER...")
    sys.stdout.flush()
    try:
        aggregated_results = filtered_df.groupby('ACCOUNT NUMBER').agg(
            # Try specifying ACCOUNT NUMBER for size, shouldn't matter but for testing
            Total_Transaction_Volume=('ACCOUNT NUMBER', 'size'), # Count rows in each group
            Total_Transaction_Value=('TRANSACTION AMOUNT', 'sum'),
            Total_Income=('Income', 'sum')
        ).reset_index()
    except Exception as agg_err:
         print(f"ERROR during groupby().agg(): {agg_err}\n{traceback.format_exc()}")
         sys.stdout.flush()
         # Fallback or return error
         return None, f"Error during data aggregation: {agg_err}", None, unique_warnings


    print("Aggregation complete.")
    print("--- Aggregated Results Sample ---")
    try:
        print(aggregated_results.head().to_string())
    except Exception as head_err:
        print(f"Could not print aggregated_results.head(): {head_err}")
    print("-------------------------------")
    sys.stdout.flush()

    # --- Prepare Summary ---
    summary_stats = f"Processed {initial_row_count} rows. Filtered down to {filtered_row_count} relevant transactions. Found {len(aggregated_results)} unique partners."
    if unique_warnings:
        max_warnings_in_summary = 5
        display_warnings = unique_warnings[:max_warnings_in_summary]
        warning_summary = "<br>Warnings encountered (showing first {}):<ul>".format(len(display_warnings)) + "".join(f"<li>{w}</li>" for w in display_warnings) + "</ul>"
        if len(unique_warnings) > max_warnings_in_summary:
            warning_summary += f"<i>...and {len(unique_warnings) - max_warnings_in_summary} more unique warnings (check logs).</i>"
        summary_stats += warning_summary

    print("--- Processing Finished ---")
    sys.stdout.flush()
    return aggregated_results, None, summary_stats, unique_warnings

# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
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
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        saved_file = False

        try:
            print(f"Attempting to save uploaded file: {filename}")
            sys.stdout.flush()
            file.save(file_path)
            saved_file = True
            print(f"File saved successfully to {file_path}")
            flash('File uploaded successfully. Processing...', 'success')
            sys.stdout.flush()

            print("Loading income rules...")
            sys.stdout.flush()
            income_rules = load_income_rules()
            if income_rules is None:
                 return redirect(url_for('index'))

            print("Calling process_transactions...")
            sys.stdout.flush()
            results_df, error_msg, summary, warnings = process_transactions(file_path, file_type, income_rules)
            print("process_transactions finished.")
            sys.stdout.flush()

            # Check if processing returned an error or None DataFrame
            if error_msg:
                flash(error_msg, 'error')
                return redirect(url_for('index'))
            if results_df is None:
                 flash("Processing failed to return results.", 'error')
                 return redirect(url_for('index'))


            # --- Optional: Debug print of final aggregated DataFrame ---
            print("--- Final Aggregated DataFrame to Render ---")
            try:
                print(results_df.to_string())
                print("\n--- Final Aggregated Info ---")
                results_df.info()
            except Exception as debug_err:
                print(f"Could not print final results_df: {debug_err}")
            print("-----------------------------------------")
            sys.stdout.flush()
            # --- End Debug Print ---


            # Store results in session for download
            # Check if results_df is valid before converting
            if isinstance(results_df, pd.DataFrame):
                session['results_df_json'] = results_df.to_json(orient='split', date_format='iso')
                session['summary_stats'] = summary
                results_list = results_df.to_dict('records')
            else:
                 # Should not happen if error checks above are fine, but as a safeguard
                 flash("Processing returned invalid results format.", 'error')
                 return redirect(url_for('index'))


            print("Rendering results page.")
            sys.stdout.flush()
            return render_template('index.html',
                                   results=results_list,
                                   summary_stats=summary)

        except Exception as e:
             print(f"ERROR in upload_file route: {e}")
             print(traceback.format_exc())
             sys.stdout.flush()
             flash(f"An unexpected error occurred during processing: {e}", 'error')
             return redirect(url_for('index'))
        finally:
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
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            results_df.to_excel(writer, index=False, sheet_name='Analysis Results')
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='transaction_analysis_results.xlsx'
        )
    except Exception as e:
        print(f"ERROR generating download file: {e}")
        print(traceback.format_exc())
        sys.stdout.flush()
        flash(f"Error generating download file: {e}", 'error')
        return redirect(url_for('index'))

# --- Run Application ---
if __name__ == '__main__':
    # Gunicorn runs the app in production using the 'app' instance
    # This block is mainly for local development testing
    # Use debug=True carefully in local dev if needed
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
