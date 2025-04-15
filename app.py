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
# *** UPDATED: Changed 'TRANSACTION TYPE' to 'PART TRAN TYPE' ***
REQUIRED_COLUMNS = {
    'ACCOUNT NUMBER', 'TRANSACTION AMOUNT', 'PART TRAN TYPE',
    'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2'
}

# --- Automatic Filter Detection Configuration ---
# Order matters for precedence if accounts overlap between types in a file
# Assumes: Leatherback -> Fincra -> Cashout 3 -> Cashout 2 -> Cashout 1
ACCOUNT_BASED_FILTER_MAP = [
    ("Leatherback or Fusion", {'3000002735', '3000003378'}),
    ("Fincra", {'3000002151'}),
    ("Cashout 3", {'3000003770'}),
    ("Cashout 2", {'010NGN45068005', '004NGN45068001'}),
    ("Cashout 1", {'3000002395', '3000001305', '3000001824'}),
]
DEFAULT_FILTER_TYPE = "Any Partner"
# --- End Configuration ---


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

    if not pd.notna(amount):
        return 0, None

    rule = rules.get(account_number)

    if rule:
        rule_type = rule.get('RuleType')
        try:
            income = 0
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
            final_income = income if pd.notna(income) else 0
            return final_income, None
        except Exception as e:
            warning = f"Calculation error for Acc {account_number}, Rule {rule_type}: {e}"
            print(f"WARNING: {warning}")
            sys.stdout.flush()
            return 0, warning
    else:
        warning = f"Unmapped Account Number: {account_number}"
        return 0, warning

def detect_filter_type_by_account(df):
    """Detects filter type by checking for known account numbers."""
    # *** UPDATED: Check for ACCOUNT NUMBER (already uppercase) ***
    if 'ACCOUNT NUMBER' not in df.columns:
        print("WARNING: 'ACCOUNT NUMBER' column not found for detection. Defaulting.")
        sys.stdout.flush()
        return DEFAULT_FILTER_TYPE

    # Account numbers already cleaned (string, stripped) in process_transactions
    present_accounts = set(df['ACCOUNT NUMBER'].unique())

    for filter_name, identifier_accounts in ACCOUNT_BASED_FILTER_MAP:
        if not present_accounts.isdisjoint(identifier_accounts):
            print(f"Detected identifying account(s) for: {filter_name}")
            sys.stdout.flush()
            return filter_name

    print(f"No specific identifying accounts found. Defaulting to: {DEFAULT_FILTER_TYPE}")
    sys.stdout.flush()
    return DEFAULT_FILTER_TYPE

# *** UPDATED: Removed file_type argument ***
def process_transactions(file_path, income_rules):
    """Reads, detects type, filters, calculates income, and aggregates."""
    print(f"--- Starting processing ---")
    sys.stdout.flush()
    try:
        print("Reading file...")
        sys.stdout.flush()
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            df = pd.read_csv(file_path, low_memory=False)
        print("File read successfully.")
        sys.stdout.flush()
    except Exception as e:
        print(f"ERROR reading file: {e}")
        sys.stdout.flush()
        return None, f"Error reading file: {e}", None, None

    initial_row_count = len(df)
    print(f"Initial rows read: {initial_row_count}")
    if initial_row_count == 0:
        return pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income']), \
               "Input file is empty.", \
               "Processed 0 rows.", \
               []

    # --- Column Name and Data Cleaning ---
    print("Cleaning column names and data...")
    sys.stdout.flush()
    original_columns = list(df.columns)
    # *** UPDATED: Ensure column names are strings before upper/strip ***
    df.columns = [str(col).upper().strip() for col in df.columns]
    current_upper_cols = set(df.columns)

    # --- Validation: Check Required Columns (using updated REQUIRED_COLUMNS) ---
    missing_cols = REQUIRED_COLUMNS - current_upper_cols
    if missing_cols:
         col_mapping_lower = {str(col).lower(): col for col in original_columns}
         required_lower = {req.lower() for req in REQUIRED_COLUMNS}
         actual_missing = [
             next((rc for rc in REQUIRED_COLUMNS if rc.lower() == req_low), req_low)
             for req_low in required_lower if req_low not in col_mapping_lower
         ]
         if actual_missing:
             err_msg = f"Missing required columns: {', '.join(actual_missing)}"
             print(f"ERROR: {err_msg}")
             sys.stdout.flush()
             return None, err_msg, None, None

    print("Required columns found.")

    # --- More Cleaning: Types, Whitespace ---
    if 'TRANSACTION AMOUNT' in df.columns:
        df['TRANSACTION AMOUNT'] = pd.to_numeric(df['TRANSACTION AMOUNT'], errors='coerce')

    # *** UPDATED: Clean 'PART TRAN TYPE' instead of 'TRANSACTION TYPE' ***
    str_cols_to_clean = ['PART TRAN TYPE', 'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2', 'ACCOUNT NUMBER']
    for col in str_cols_to_clean:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('').str.strip()

    # *** UPDATED: Apply case changes AFTER cleaning using correct column name ***
    if 'PART TRAN TYPE' in df.columns:
         df['PART TRAN TYPE'] = df['PART TRAN TYPE'].str.upper()
    if 'TRAN_PARTICULAR' in df.columns:
         df['TRAN_PARTICULAR'] = df['TRAN_PARTICULAR'].str.upper()
    print("Data cleaning finished.")

    # --- Automatic Filter Type Detection ---
    print("Detecting filter type based on account numbers...")
    sys.stdout.flush()
    detected_file_type = detect_filter_type_by_account(df)
    # ----------------------------------------

    # --- FR3: Transaction Filtering ---
    print(f"--- Applying Filters for Detected Type: {detected_file_type} ---")
    sys.stdout.flush()
    try:
        # --- Base Conditions (Check column existence) ---
        f_amount_notna = df['TRANSACTION AMOUNT'].notna() if 'TRANSACTION AMOUNT' in df.columns else pd.Series(True, index=df.index)
        # *** UPDATED: Use PART TRAN TYPE ***
        f_type_is_c = (df['PART TRAN TYPE'] == 'C') if 'PART TRAN TYPE' in df.columns else pd.Series(True, index=df.index)
        f_tp2_10_digits = df['TRAN_PARTICULAR_2'].str.match(r'^\d{10}') if 'TRAN_PARTICULAR_2' in df.columns else pd.Series(True, index=df.index)
        f_not_rvsl = ~df['TRAN_PARTICULAR'].str.startswith('RVSL') if 'TRAN_PARTICULAR' in df.columns else pd.Series(True, index=df.index)
        f_not_reversal = ~df['TRAN_PARTICULAR'].str.startswith('REVERSAL') if 'TRAN_PARTICULAR' in df.columns else pd.Series(True, index=df.index)
        f_tp2_not_ft = ~df['TRAN_PARTICULAR_2'].str.startswith('FT') if 'TRAN_PARTICULAR_2' in df.columns else pd.Series(True, index=df.index)

        # Apply filters based on detected type
        if detected_file_type == 'Any Partner':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )
        elif detected_file_type == 'Fincra':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            f_not_name = (~df['TRAN_PARTICULAR'].str.startswith('NAME')) if 'TRAN_PARTICULAR' in df.columns else pd.Series(True, index=df.index)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal & f_not_name )
        elif detected_file_type == 'Leatherback or Fusion':
            f_amount = (df['TRANSACTION AMOUNT'] >= 10000)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )
        elif detected_file_type == 'Cashout 1':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            f_not_gl = (~df['TRAN_PARTICULAR'].str.startswith('GL')) if 'TRAN_PARTICULAR' in df.columns else pd.Series(True, index=df.index)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_not_rvsl & f_not_reversal & f_not_gl )
        elif detected_file_type == 'Cashout 2':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c )
        elif detected_file_type == 'Cashout 3':
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            # Handle potential NaN before startswith
            f_is_nip = df['TRAN_PARTICULAR'].str.startswith('"NIP"') if 'TRAN_PARTICULAR' in df.columns else pd.Series(False, index=df.index)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_not_rvsl & f_not_reversal & f_is_nip & f_tp2_not_ft )
        else: # Should not happen if default is set, but safety catch
            print(f"WARNING: Unknown detected_file_type '{detected_file_type}'. Applying default 'Any Partner'.")
            sys.stdout.flush()
            f_amount = (df['TRANSACTION AMOUNT'] >= 100)
            combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )

        # Apply the combined filter
        filtered_df = df[combined_filter].copy()

    except KeyError as ke:
        err_msg = f"Filtering error: Column not found - {ke}. Check input file structure and ensure '{ke}' is present."
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
        return pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income']), \
               "No transactions passed the filtering criteria.", \
               f"Processed {initial_row_count} rows ({detected_file_type} rules applied), Filtered down to 0 rows.", \
               [] # No warnings if no rows to process

    # --- FR4: Per-Transaction Income Calculation ---
    print("Calculating income per transaction...")
    sys.stdout.flush()
    warnings = []
    income_results_list = []
    if income_rules is None:
         print("ERROR: Income rules not loaded, cannot calculate income.")
         sys.stdout.flush()
         filtered_df['Income'] = 0
    else:
        for index, row in filtered_df.iterrows():
            income, warning = calculate_income(row, income_rules)
            income_results_list.append(income)
            if warning:
                warnings.append(warning)
        filtered_df['Income'] = income_results_list

    unique_warnings = sorted(list(set(warnings)))
    print(f"Income calculation complete. Found {len(unique_warnings)} unique warnings.")
    sys.stdout.flush()

    # --- FR4.4 & FR4.5: Aggregation by Partner (ACCOUNT NUMBER) ---
    print("Aggregating results by ACCOUNT NUMBER...")
    sys.stdout.flush()
    try:
        # Check if filtered_df is empty before attempting aggregation
        if filtered_df.empty:
            aggregated_results = pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income'])
        else:
             aggregated_results = filtered_df.groupby('ACCOUNT NUMBER').agg(
                Total_Transaction_Volume=('ACCOUNT NUMBER', 'size'),
                Total_Transaction_Value=('TRANSACTION AMOUNT', 'sum'),
                Total_Income=('Income', 'sum')
             ).reset_index()
    except Exception as agg_err:
         print(f"ERROR during groupby().agg(): {agg_err}\n{traceback.format_exc()}")
         sys.stdout.flush()
         return None, f"Error during data aggregation: {agg_err}", None, unique_warnings

    print("Aggregation complete.")
    # Optional: print(aggregated_results.head().to_string())

    # --- Prepare Summary ---
    summary_stats = f"Processed {initial_row_count} rows. Filtered down to {filtered_row_count} relevant transactions ({detected_file_type} rules applied). Found {len(aggregated_results)} unique partners."
    if unique_warnings:
        max_warnings_in_summary = 5
        display_warnings = unique_warnings[:max_warnings_in_summary]
        warning_summary = "<br>Warnings encountered (first {}):<ul>".format(len(display_warnings)) + "".join(f"<li>{w}</li>" for w in display_warnings) + "</ul>"
        if len(unique_warnings) > max_warnings_in_summary:
            warning_summary += f"<i>...and {len(unique_warnings) - max_warnings_in_summary} more (check logs).</i>"
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

    # ** REMOVED file_type input check from form **

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
            flash('File uploaded successfully. Detecting type & Processing...', 'success')
            sys.stdout.flush()

            print("Loading income rules...")
            sys.stdout.flush()
            income_rules = load_income_rules()
            if income_rules is None:
                 return redirect(url_for('index'))

            print("Calling process_transactions...")
            sys.stdout.flush()
            # Pass only file_path and income_rules
            results_df, error_msg, summary, warnings = process_transactions(file_path, income_rules)
            print("process_transactions finished.")
            sys.stdout.flush()

            if error_msg:
                flash(error_msg, 'error')
                return redirect(url_for('index'))
            if results_df is None:
                 flash("Processing failed to return results.", 'error')
                 return redirect(url_for('index'))

            # Store results in session for download
            if isinstance(results_df, pd.DataFrame):
                # Define columns expected by template/download using underscores
                session_df_columns = ['ACCOUNT NUMBER', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income']
                # Ensure aggregated results actually has these columns before selecting
                # Handle case where results_df might be empty but valid
                if all(col in results_df.columns for col in session_df_columns):
                     session['results_df_json'] = results_df[session_df_columns].to_json(orient='split', date_format='iso')
                elif results_df.empty:
                     session['results_df_json'] = results_df.to_json(orient='split', date_format='iso') # Store empty df okay
                else:
                     # This case indicates a potential logic error where agg results missing columns
                     print("WARNING: Aggregated results missing expected columns. Storing raw aggregated.")
                     sys.stdout.flush()
                     session['results_df_json'] = results_df.to_json(orient='split', date_format='iso')

                session['summary_stats'] = summary
                results_list = results_df.to_dict('records')
            else:
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
            # Rename columns for user-friendly download file
            results_df.columns = ['ACCOUNT NUMBER', 'Total Transaction Volume', 'Total Transaction Value', 'Total Income']
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
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
