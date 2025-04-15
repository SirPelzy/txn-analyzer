import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from werkzeug.utils import secure_filename
import io # For sending file in memory
import sys # To flush print statements for logs
import traceback # For detailed error logging
import time # To measure processing time

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'csv'}
INCOME_RULES_FILE = 'income_rules.csv'
REQUIRED_COLUMNS = { # Updated column name
    'ACCOUNT NUMBER', 'TRANSACTION AMOUNT', 'PART TRAN TYPE',
    'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2'
}
CHUNK_SIZE = 10000 # Process N rows at a time (Adjust based on memory/performance)

# --- Automatic Filter Detection Configuration ---
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
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24))
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024 # Increase upload limit slightly if needed

# --- Helper Functions (allowed_file, load_income_rules, calculate_income remain mostly the same) ---

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
        flash(f"Error: Income rules file '{filepath}' not found.", 'error')
        return None
    except Exception as e:
        print(f"ERROR loading income rules: {e}")
        flash(f"Error loading income rules: {e}", 'error')
        return None

def calculate_income(row, rules):
    """Calculates income for a single transaction row based on loaded rules."""
    account_number = str(row.get('ACCOUNT NUMBER', '')).strip()
    amount = row.get('TRANSACTION AMOUNT')
    if not pd.notna(amount): return 0, None
    rule = rules.get(account_number)
    if rule:
        rule_type = rule.get('RuleType')
        try:
            income = 0
            if rule_type == 'Fixed': income = rule.get('FixedAmount', 0)
            elif rule_type == 'PercentageCap':
                percentage = rule.get('Percentage', 0); cap = rule.get('Cap')
                income = amount * percentage
                if pd.notna(cap) and income > cap: income = cap
            elif rule_type == 'ConditionalFixed':
                threshold = rule.get('ThresholdAmount'); fixed_amount = rule.get('FixedAmount', 0)
                if pd.notna(threshold) and amount > threshold: income = fixed_amount
                else: income = 0
            elif rule_type == 'None': income = 0
            final_income = income if pd.notna(income) else 0
            return final_income, None
        except Exception as e: return 0, f"CalcError Acc {account_number}: {e}" # Return warning
    else: return 0, f"Unmapped Acc: {account_number}" # Return warning

def detect_filter_type_by_account(df):
    """Detects filter type by checking for known account numbers in a DataFrame chunk."""
    if 'ACCOUNT NUMBER' not in df.columns: return DEFAULT_FILTER_TYPE
    present_accounts = set(df['ACCOUNT NUMBER'].astype(str).str.strip().unique())
    for filter_name, identifier_accounts in ACCOUNT_BASED_FILTER_MAP:
        if not present_accounts.isdisjoint(identifier_accounts):
            print(f"Detected identifying account(s) for: {filter_name}")
            sys.stdout.flush()
            return filter_name
    print(f"No specific identifying accounts found in chunk. Defaulting to: {DEFAULT_FILTER_TYPE}")
    sys.stdout.flush()
    return DEFAULT_FILTER_TYPE

# --- Main Processing Function (Refactored for Chunking) ---

def process_transactions_chunked(file_path, income_rules):
    """
    Reads, detects type, filters, calculates income, and aggregates transaction data
    IN CHUNKS to handle large files. Optimized for CSV.
    """
    start_time = time.time()
    print(f"--- Starting chunked processing (Chunk Size: {CHUNK_SIZE}) ---")
    sys.stdout.flush()

    all_chunk_aggregated_results = []
    total_rows_processed = 0
    total_rows_filtered = 0
    detected_file_type = None
    unique_warnings = set()
    reader = None # Initialize reader outside try

    try:
        # --- Setup Reader ---
        print("Setting up file reader...")
        sys.stdout.flush()
        if file_path.endswith('.xlsx'):
            # NOTE: Chunking Excel is less standard. This might still load large parts.
            # Consider libraries like 'dask' or converting to CSV first for very large Excel.
            # For simplicity, we'll try iterating sheets or using pandas' limited chunking if available.
            # This basic implementation might fall back to reading all at once for Excel.
            try:
                 # Attempt to read in chunks (may not work reliably for all Excel types/engines)
                 reader = pd.read_excel(file_path, engine='openpyxl', chunksize=CHUNK_SIZE)
                 print("Reading Excel file in chunks (experimental)...")
            except TypeError: # If chunksize not supported by engine for this file
                 print("WARNING: Chunking not directly supported for this Excel file with openpyxl. Reading whole file.")
                 df_full = pd.read_excel(file_path, engine='openpyxl')
                 reader = [df_full] # Treat the whole DataFrame as a single chunk
        else:
            # CSV chunking is reliable
            reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=False)
            print("Reading CSV file in chunks...")
        sys.stdout.flush()

        # --- Process Chunks ---
        first_chunk = True
        original_columns = [] # Store original column names from first chunk

        for i, chunk_df in enumerate(reader):
            chunk_start_time = time.time()
            print(f"\nProcessing Chunk {i+1}...")
            sys.stdout.flush()

            chunk_rows_initial = len(chunk_df)
            total_rows_processed += chunk_rows_initial
            if chunk_rows_initial == 0:
                 print("Empty chunk, skipping.")
                 continue

            # --- Column Name Cleaning & Validation (on first chunk) ---
            if first_chunk:
                original_columns = list(chunk_df.columns)
                chunk_df.columns = [str(col).upper().strip() for col in chunk_df.columns]
                current_upper_cols = set(chunk_df.columns)
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
                        return None, err_msg, None, None
                print("Required columns verified in first chunk.")
            else:
                # Apply consistent naming to subsequent chunks
                chunk_df.columns = [str(col).upper().strip() for col in chunk_df.columns]

            # --- Data Cleaning (Types, Whitespace) ---
            if 'TRANSACTION AMOUNT' in chunk_df.columns:
                chunk_df['TRANSACTION AMOUNT'] = pd.to_numeric(chunk_df['TRANSACTION AMOUNT'], errors='coerce')
            str_cols_to_clean = ['PART TRAN TYPE', 'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2', 'ACCOUNT NUMBER']
            for col in str_cols_to_clean:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str).fillna('').str.strip()
            if 'PART TRAN TYPE' in chunk_df.columns:
                chunk_df['PART TRAN TYPE'] = chunk_df['PART TRAN TYPE'].str.upper()
            if 'TRAN_PARTICULAR' in chunk_df.columns:
                chunk_df['TRAN_PARTICULAR'] = chunk_df['TRAN_PARTICULAR'].str.upper()

            # --- Automatic Filter Type Detection (on first chunk) ---
            if first_chunk:
                print("Detecting filter type from first chunk...")
                detected_file_type = detect_filter_type_by_account(chunk_df)
                first_chunk = False # Only detect once

            # --- Apply Filters ---
            if not detected_file_type: # Safety check if detection failed somehow
                 print("ERROR: Filter type could not be detected. Aborting.")
                 return None, "Could not detect filter type.", None, None

            try:
                # Define base conditions (check column existence within chunk)
                f_amount_notna = chunk_df['TRANSACTION AMOUNT'].notna() if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_type_is_c = (chunk_df['PART TRAN TYPE'] == 'C') if 'PART TRAN TYPE' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_tp2_10_digits = chunk_df['TRAN_PARTICULAR_2'].str.match(r'^\d{10}') if 'TRAN_PARTICULAR_2' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_not_rvsl = ~chunk_df['TRAN_PARTICULAR'].str.startswith('RVSL') if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_not_reversal = ~chunk_df['TRAN_PARTICULAR'].str.startswith('REVERSAL') if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_tp2_not_ft = ~chunk_df['TRAN_PARTICULAR_2'].str.startswith('FT') if 'TRAN_PARTICULAR_2' in chunk_df.columns else pd.Series(True, index=chunk_df.index)

                # Select filter logic based on detected type
                if detected_file_type == 'Any Partner':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )
                elif detected_file_type == 'Fincra':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100)
                    f_not_name = (~chunk_df['TRAN_PARTICULAR'].str.startswith('NAME')) if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal & f_not_name )
                elif detected_file_type == 'Leatherback or Fusion':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 10000)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )
                elif detected_file_type == 'Cashout 1':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100)
                    f_not_gl = (~chunk_df['TRAN_PARTICULAR'].str.startswith('GL')) if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_not_rvsl & f_not_reversal & f_not_gl )
                elif detected_file_type == 'Cashout 2':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c )
                elif detected_file_type == 'Cashout 3':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100)
                    f_is_nip = chunk_df['TRAN_PARTICULAR'].str.startswith('"NIP"') if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(False, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_not_rvsl & f_not_reversal & f_is_nip & f_tp2_not_ft )
                else: # Default case (should match Any Partner ideally)
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )

                # Apply the combined filter
                filtered_chunk_df = chunk_df[combined_filter].copy() # Use copy

            except KeyError as ke:
                err_msg = f"Filtering error in chunk {i+1}: Column not found - {ke}."
                print(f"ERROR: {err_msg}")
                sys.stdout.flush()
                # Decide whether to skip chunk or abort all processing
                # For now, let's abort:
                return None, err_msg, None, None
            except Exception as filter_err:
                err_msg = f"Filtering error in chunk {i+1}: {filter_err}"
                print(f"ERROR: {err_msg} \n{traceback.format_exc()}")
                sys.stdout.flush()
                return None, err_msg, None, None

            chunk_rows_filtered = len(filtered_chunk_df)
            total_rows_filtered += chunk_rows_filtered
            print(f"Chunk {i+1}: Processed {chunk_rows_initial} rows -> Filtered down to {chunk_rows_filtered} rows.")
            sys.stdout.flush()

            if chunk_rows_filtered > 0:
                # --- Calculate Income for Chunk ---
                if income_rules is None:
                    print("ERROR: Income rules not loaded. Skipping income calculation.")
                    filtered_chunk_df['Income'] = 0
                else:
                    income_col = []
                    warnings_col = []
                    for _, row in filtered_chunk_df.iterrows(): # iterrows is okay for chunks
                        income, warning = calculate_income(row, income_rules)
                        income_col.append(income)
                        if warning: warnings_col.append(warning)
                    filtered_chunk_df['Income'] = income_col
                    unique_warnings.update(warnings_col) # Add chunk warnings to overall set

                # --- Aggregate Chunk Results ---
                try:
                     # Check if necessary columns exist before aggregation
                     agg_cols_present = {'ACCOUNT NUMBER', 'TRANSACTION AMOUNT', 'Income'} <= set(filtered_chunk_df.columns)
                     if agg_cols_present:
                         chunk_aggregated = filtered_chunk_df.groupby('ACCOUNT NUMBER').agg(
                            Total_Transaction_Volume=('ACCOUNT NUMBER', 'size'),
                            Total_Transaction_Value=('TRANSACTION AMOUNT', 'sum'),
                            Total_Income=('Income', 'sum')
                         ).reset_index()
                         all_chunk_aggregated_results.append(chunk_aggregated)
                         print(f"Chunk {i+1}: Aggregated {len(chunk_aggregated)} partner summaries.")
                     else:
                         print(f"WARNING: Chunk {i+1} missing columns needed for aggregation. Skipping aggregation for this chunk.")

                except Exception as agg_err:
                    print(f"ERROR during aggregation in chunk {i+1}: {agg_err}\n{traceback.format_exc()}")
                    # Decide how to handle: skip chunk aggregation or abort? Let's skip.
                    flash(f"Warning: Error aggregating chunk {i+1}. Results might be incomplete.", "warning")

            chunk_end_time = time.time()
            print(f"Chunk {i+1} processing time: {chunk_end_time - chunk_start_time:.2f} seconds")
            sys.stdout.flush()
            # End of chunk loop

        # --- Final Aggregation ---
        print("\nCombining results from all chunks...")
        sys.stdout.flush()
        if not all_chunk_aggregated_results:
            print("No results were aggregated from any chunk.")
            if total_rows_filtered > 0:
                 # This might indicate an aggregation error occurred in all chunks
                 error_msg = "Processing completed, but failed to aggregate results."
                 summary = f"Processed {total_rows_processed} rows. Filtered down to {total_rows_filtered} relevant transactions ({detected_file_type} rules applied). Aggregation failed."
                 return None, error_msg, summary, sorted(list(unique_warnings))
            else:
                 # No rows passed filtering in any chunk
                 return pd.DataFrame(columns=['ACCOUNT NUMBER', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income']), \
                        "No transactions passed the filtering criteria.", \
                        f"Processed {total_rows_processed} rows ({detected_file_type} rules applied), Filtered down to 0 rows.", \
                        sorted(list(unique_warnings))

        # Concatenate results from all chunks
        final_df_concat = pd.concat(all_chunk_aggregated_results, ignore_index=True)

        # Re-aggregate the concatenated results to sum correctly across chunks
        print("Performing final aggregation across all chunks...")
        final_aggregated_results = final_df_concat.groupby('ACCOUNT NUMBER').agg(
            Total_Transaction_Volume=('Total_Transaction_Volume', 'sum'), # Sum the sizes from chunks
            Total_Transaction_Value=('Total_Transaction_Value', 'sum'),
            Total_Income=('Total_Income', 'sum')
        ).reset_index()
        print("Final aggregation complete.")
        sys.stdout.flush()

        # --- Prepare Summary ---
        summary_stats = f"Processed {total_rows_processed} rows. Filtered down to {total_rows_filtered} relevant transactions ({detected_file_type} rules applied). Found {len(final_aggregated_results)} unique partners."
        if unique_warnings:
            # Add warning summary logic as before...
             max_warnings_in_summary = 5
             display_warnings = sorted(list(unique_warnings))[:max_warnings_in_summary]
             warning_summary = "<br>Warnings encountered (first {}):<ul>".format(len(display_warnings)) + "".join(f"<li>{w}</li>" for w in display_warnings) + "</ul>"
             if len(unique_warnings) > max_warnings_in_summary:
                 warning_summary += f"<i>...and {len(unique_warnings) - max_warnings_in_summary} more (check logs).</i>"
             summary_stats += warning_summary

        total_time = time.time() - start_time
        print(f"--- Processing Finished ---")
        print(f"Total processing time: {total_time:.2f} seconds")
        sys.stdout.flush()
        return final_aggregated_results, None, summary_stats, sorted(list(unique_warnings))

    except Exception as e:
        # Catch errors during reader setup or loop iteration
        print(f"ERROR during chunk processing: {e}")
        print(traceback.format_exc())
        sys.stdout.flush()
        return None, f"Error processing file in chunks: {e}", None, None


# --- Flask Routes (upload_file updated to call chunked function) ---

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
    session.pop('results_df_json', None)
    session.pop('summary_stats', None)
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file upload, processing using chunks, and renders results."""
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
            flash('File uploaded successfully. Processing large file (this may take time)...', 'info') # Updated flash
            sys.stdout.flush()

            print("Loading income rules...")
            sys.stdout.flush()
            income_rules = load_income_rules()
            if income_rules is None:
                 return redirect(url_for('index'))

            print("Calling process_transactions_chunked...")
            sys.stdout.flush()
            # *** UPDATED: Call the chunked processing function ***
            results_df, error_msg, summary, warnings = process_transactions_chunked(file_path, income_rules)
            print("Chunked processing finished.")
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
                if all(col in results_df.columns for col in session_df_columns):
                     session['results_df_json'] = results_df[session_df_columns].to_json(orient='split', date_format='iso')
                elif results_df.empty:
                     session['results_df_json'] = results_df.to_json(orient='split', date_format='iso')
                else:
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

# --- Download Route (remains the same, but adjusted column rename) ---
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
            # Rename columns for user-friendly download file *if needed*
            # Assuming template uses underscores, keep them consistent or rename here
            results_df.columns = ['ACCOUNT NUMBER', 'Total Transaction Volume', 'Total Transaction Value', 'Total Income'] # Rename for output
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
