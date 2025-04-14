import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from werkzeug.utils import secure_filename
import io # For sending file in memory

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
app.secret_key = os.urandom(24) # Important for flash messages and session
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER) # Create upload folder if it doesn't exist
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 70 * 1024 * 1024 # Allow ~70MB upload

# --- Helper Functions ---

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_income_rules(filepath=INCOME_RULES_FILE):
    """Loads income rules from CSV into a dictionary for quick lookup."""
    try:
        rules_df = pd.read_csv(filepath)
        # Convert ACCOUNT NUMBER to string for consistent matching
        rules_df['ACCOUNT NUMBER'] = rules_df['ACCOUNT NUMBER'].astype(str)
        rules_df.set_index('ACCOUNT NUMBER', inplace=True)
        # Convert relevant columns to numeric, handling potential errors
        for col in ['FixedAmount', 'Percentage', 'Cap', 'ThresholdAmount']:
            if col in rules_df.columns:
                 rules_df[col] = pd.to_numeric(rules_df[col], errors='coerce')
        return rules_df.to_dict('index')
    except FileNotFoundError:
        flash(f"Error: Income rules file '{filepath}' not found.", 'error')
        return None
    except Exception as e:
        flash(f"Error loading income rules: {e}", 'error')
        return None

def calculate_income(row, rules):
    """Calculates income for a single transaction row based on loaded rules."""
    account_number = str(row['ACCOUNT NUMBER']) # Ensure string comparison
    amount = row['TRANSACTION AMOUNT']
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
            return 0, warning # Or return a specific error indicator if needed
    else:
        # Unmapped account number - FR4.2.5
        warning = f"Unmapped Account Number: {account_number}"
        return 0, warning

def process_transactions(file_path, file_type, income_rules):
    """Reads, filters, calculates income, and aggregates transaction data."""
    try:
        if file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            df = pd.read_csv(file_path)
    except Exception as e:
        return None, f"Error reading file: {e}", None, None

    initial_row_count = len(df)

    # --- FR2: Data Parsing & Validation ---
    # Check for required columns (case-insensitive check)
    missing_cols = REQUIRED_COLUMNS - set(col.upper() for col in df.columns)
    if missing_cols:
        # Try matching case-insensitively before failing
        col_mapping = {col.upper(): col for col in df.columns}
        actual_missing = []
        for req_col in REQUIRED_COLUMNS:
             if req_col not in col_mapping:
                 actual_missing.append(req_col)
        if actual_missing:
             return None, f"Missing required columns: {', '.join(actual_missing)}", None, None

    # Rename columns to a standard case for consistency (optional but good practice)
    df.columns = [col.upper() for col in df.columns]

    # Ensure essential numeric columns are numeric, coercing errors
    df['TRANSACTION AMOUNT'] = pd.to_numeric(df['TRANSACTION AMOUNT'], errors='coerce')
    # Optionally handle rows with conversion errors (e.g., log, remove, or flag)
    # df.dropna(subset=['TRANSACTION AMOUNT'], inplace=True) # Example: remove rows with invalid amounts

    # --- FR3: Transaction Filtering ---
    # Convert relevant text columns to string type to avoid errors with .str accessor
    for col in ['TRANSACTION TYPE', 'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2']:
        if col in df.columns:
            df[col] = df[col].astype(str).fillna('') # Handle NaN values before string operations

    # Base filters (applied to most types)
    base_filter = (
        (df['TRANSACTION TYPE'].str.upper() == 'C') &
        (df['TRAN_PARTICULAR_2'].str.match(r'^\d{10}')) & # Starts with exactly 10 digits
        (~df['TRAN_PARTICULAR'].str.upper().str.startswith('RVSL')) & # NOT starts with RVSL
        (~df['TRAN_PARTICULAR'].str.upper().str.startswith('REVERSAL')) # NOT starts with Reversal
    )

    # Type-specific filters
    if file_type == 'Any Partner':
        specific_filter = (df['TRANSACTION AMOUNT'] >= 100)
    elif file_type == 'Fincra':
        specific_filter = (
            (df['TRANSACTION AMOUNT'] >= 100) &
            (~df['TRAN_PARTICULAR'].str.upper().str.startswith('NAME')) # Additional Fincra filter
        )
    elif file_type == 'Leatherback or Fusion':
        specific_filter = (df['TRANSACTION AMOUNT'] >= 10000)
        # Note: The base filter already handles the 'RVSL' part for this type
    elif file_type == 'Cashout':
        specific_filter = (
            (df['TRANSACTION AMOUNT'] >= 100) &
            (~df['TRAN_PARTICULAR'].str.upper().str.startswith('FT')) # Changed Cashout filter
        )
        # Note: The base filter handles 'TRANSACTION TYPE', 'TRAN_PARTICULAR_2', and 'Reversals'
    else:
        return None, f"Invalid file type selection: {file_type}", None, None

    # Combine base and specific filters
    # Handle potential NaN in TRANSACTION AMOUNT before comparison
    combined_filter = base_filter & specific_filter & df['TRANSACTION AMOUNT'].notna()
    filtered_df = df[combined_filter].copy() # Use .copy() to avoid SettingWithCopyWarning

    filtered_row_count = len(filtered_df)

    if filtered_df.empty:
        return pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total Transaction Volume', 'Total Transaction Value', 'Total Income']), \
               "No transactions passed the filtering criteria.", \
               f"Processed {initial_row_count} rows, Filtered down to 0 rows.", \
               [] # No warnings if no rows to process

    # --- FR4: Per-Transaction Income Calculation ---
    warnings = []
    income_results = filtered_df.apply(
        lambda row: calculate_income(row, income_rules),
        axis=1
    )
    # Separate income and warnings
    filtered_df['Income'] = [res[0] for res in income_results]
    row_warnings = [res[1] for res in income_results if res[1] is not None]
    unique_warnings = sorted(list(set(row_warnings))) # Get unique warnings

    # --- FR4.4 & FR4.5: Aggregation by Partner (ACCOUNT NUMBER) ---
    # Ensure ACCOUNT NUMBER is string for consistent grouping
    filtered_df['ACCOUNT NUMBER'] = filtered_df['ACCOUNT NUMBER'].astype(str)

    aggregated_results = filtered_df.groupby('ACCOUNT NUMBER').agg(
        Total_Transaction_Volume=('ACCOUNT NUMBER', 'size'), # Count rows in each group
        Total_Transaction_Value=('TRANSACTION AMOUNT', 'sum'),
        Total_Income=('Income', 'sum')
    ).reset_index() # Convert grouped output back to DataFrame

    # --- Prepare Summary ---
    summary_stats = f"Processed {initial_row_count} rows. Filtered down to {filtered_row_count} relevant transactions. Found {len(aggregated_results)} unique partners."
    if unique_warnings:
        warning_summary = "<br>Warnings encountered:<ul>" + "".join(f"<li>{w}</li>" for w in unique_warnings) + "</ul>"
        summary_stats += warning_summary
        # Also flash unique warnings for visibility
        for warn in unique_warnings:
            flash(warn, 'warning')

    return aggregated_results, None, summary_stats, unique_warnings # Return results, no error, summary, warnings

# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    # Clear previous results from session on new visit
    session.pop('results_df_json', None)
    session.pop('summary_stats', None)
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
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
        # Save file temporarily (optional, could process in memory for smaller files)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        try:
            file.save(file_path)
            flash('File uploaded successfully. Processing...', 'success')

            # Load income rules
            income_rules = load_income_rules()
            if income_rules is None:
                 # Error flashed in load_income_rules
                 os.remove(file_path) # Clean up uploaded file
                 return redirect(url_for('index'))

            # Process the file
            results_df, error_msg, summary, warnings = process_transactions(file_path, file_type, income_rules)

            os.remove(file_path) # Clean up uploaded file after processing

            if error_msg:
                flash(error_msg, 'error')
                return redirect(url_for('index'))

            # Store results in session for download
            session['results_df_json'] = results_df.to_json(orient='split', date_format='iso')
            session['summary_stats'] = summary

            # Render results on the page
            return render_template('index.html',
                                   results=results_df.to_dict('records'),
                                   summary_stats=summary)

        except Exception as e:
             flash(f"An unexpected error occurred: {e}", 'error')
             # Clean up if file was saved
             if 'file_path' in locals() and os.path.exists(file_path):
                 os.remove(file_path)
             return redirect(url_for('index'))
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
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            results_df.to_excel(writer, index=False, sheet_name='Analysis Results')
        output.seek(0) # Rewind the buffer

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='transaction_analysis_results.xlsx' # Changed from attachment_filename
        )
    except Exception as e:
        flash(f"Error generating download file: {e}", 'error')
        return redirect(url_for('index'))


# --- Run Application ---
if __name__ == '__main__':
    # Use debug=True only for development. Gunicorn will handle production.
    # Consider using Waitress for simpler Windows development: pip install waitress; waitress-serve --listen=*:5000 app:app
    app.run(debug=True)
