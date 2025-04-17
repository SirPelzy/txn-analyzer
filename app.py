# -*- coding: utf-8 -*-
import os
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session
from werkzeug.utils import secure_filename
import io # For sending file in memory
import sys # To flush print statements for logs
import traceback # For detailed error logging
import time # To measure processing time
from dotenv import load_dotenv # For local environment variables

# Load .env file if it exists (for local development)
load_dotenv()

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'csv'}
INCOME_RULES_FILE = 'income_rules.csv'
# *** UPDATED: Added 'ACCOUNT NAME' ***
REQUIRED_COLUMNS = {
    'ACCOUNT NUMBER', 'ACCOUNT NAME', 'TRANSACTION AMOUNT', 'PART TRAN TYPE',
    'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2'
}
CHUNK_SIZE = 10000 # Process N rows at a time

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
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24)) # Use SECRET_KEY from .env or generate random
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 150 * 1024 * 1024

# --- Helper Functions (allowed_file, load_income_rules, calculate_income unchanged) ---

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
        except Exception as e: return 0, f"CalcError Acc {account_number}: {e}"
    else: return 0, f"Unmapped Acc: {account_number}"

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

# --- Main Processing Function (Chunked) ---

def process_transactions_chunked(file_path, income_rules):
    start_time = time.time()
    print(f"--- Starting chunked processing (Chunk Size: {CHUNK_SIZE}) ---")
    sys.stdout.flush()

    all_chunk_aggregated_results = []
    total_rows_processed = 0
    total_rows_filtered = 0
    detected_file_type = None
    unique_warnings = set()
    reader = None
    required_cols_validated = False # Flag to check cols only once

    try:
        print("Setting up file reader...")
        sys.stdout.flush()
        if file_path.endswith('.xlsx'):
            # Attempt to read only potentially needed columns for Excel for efficiency
            # Note: Reading specific columns AND chunking Excel is difficult with pandas directly
            print("WARNING: Reading full Excel file. Chunking works best with CSV.")
            # Consider reading schema first if possible or just read all
            df_full = pd.read_excel(file_path, engine='openpyxl')
            reader = [df_full] # Treat as one chunk
        else:
            # Chunking for CSV
            reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=False)
            print("Reading CSV file in chunks...")
        sys.stdout.flush()

        # --- Process Chunks ---
        first_chunk = True
        original_columns = []

        for i, chunk_df in enumerate(reader):
            chunk_start_time = time.time()
            print(f"\nProcessing Chunk {i+1}...")
            sys.stdout.flush()

            chunk_rows_initial = len(chunk_df)
            total_rows_processed += chunk_rows_initial
            if chunk_rows_initial == 0: continue

            # --- Column Name Cleaning & Validation (on first chunk) ---
            if first_chunk:
                original_columns = list(chunk_df.columns)
                chunk_df.columns = [str(col).upper().strip() for col in chunk_df.columns]
                current_upper_cols = set(chunk_df.columns)
                # *** UPDATED: Use new REQUIRED_COLUMNS including ACCOUNT NAME ***
                missing_cols = REQUIRED_COLUMNS - current_upper_cols
                if missing_cols:
                    col_mapping_lower = {str(col).lower(): col for col in original_columns}
                    required_lower = {req.lower() for req in REQUIRED_COLUMNS}
                    actual_missing = [
                        next((rc for rc in REQUIRED_COLUMNS if rc.lower() == req_low), req_low)
                        for req_low in required_lower if req_low not in col_mapping_lower ]
                    if actual_missing:
                        err_msg = f"Missing required columns: {', '.join(actual_missing)}"
                        print(f"ERROR: {err_msg}")
                        return None, err_msg, None, None
                print("Required columns verified in first chunk.")
                required_cols_validated = True
            else:
                # Apply consistent naming
                chunk_df.columns = [str(col).upper().strip() for col in chunk_df.columns]

            if not required_cols_validated: # Should have been caught, but safety check
                 return None, "Required columns missing.", None, None

            # --- Data Cleaning ---
            if 'TRANSACTION AMOUNT' in chunk_df.columns:
                chunk_df['TRANSACTION AMOUNT'] = pd.to_numeric(chunk_df['TRANSACTION AMOUNT'], errors='coerce')
            # *** UPDATED: Added ACCOUNT NAME to cleaning list ***
            str_cols_to_clean = ['PART TRAN TYPE', 'TRAN_PARTICULAR', 'TRAN_PARTICULAR_2', 'ACCOUNT NUMBER', 'ACCOUNT NAME']
            for col in str_cols_to_clean:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str).fillna('').str.strip()
            # Case changes after cleaning
            if 'PART TRAN TYPE' in chunk_df.columns:
                 chunk_df['PART TRAN TYPE'] = chunk_df['PART TRAN TYPE'].str.upper()
            if 'TRAN_PARTICULAR' in chunk_df.columns:
                 chunk_df['TRAN_PARTICULAR'] = chunk_df['TRAN_PARTICULAR'].str.upper()

            # --- Automatic Filter Type Detection (on first chunk) ---
            if first_chunk:
                print("Detecting filter type from first chunk...")
                detected_file_type = detect_filter_type_by_account(chunk_df)
                first_chunk = False

            if not detected_file_type:
                 return None, "Could not detect filter type.", None, None

            # --- Apply Filters (same logic as before) ---
            try:
                f_amount_notna = chunk_df['TRANSACTION AMOUNT'].notna() if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_type_is_c = (chunk_df['PART TRAN TYPE'] == 'C') if 'PART TRAN TYPE' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_tp2_10_digits = chunk_df['TRAN_PARTICULAR_2'].str.match(r'^\d{10}') if 'TRAN_PARTICULAR_2' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_not_rvsl = ~chunk_df['TRAN_PARTICULAR'].str.startswith('RVSL') if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_not_reversal = ~chunk_df['TRAN_PARTICULAR'].str.startswith('REVERSAL') if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                f_tp2_not_ft = ~chunk_df['TRAN_PARTICULAR_2'].str.startswith('FT') if 'TRAN_PARTICULAR_2' in chunk_df.columns else pd.Series(True, index=chunk_df.index)

                # Select filter logic based on detected type (Ensure columns exist)
                if detected_file_type == 'Any Partner':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )
                elif detected_file_type == 'Fincra':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    f_not_name = (~chunk_df['TRAN_PARTICULAR'].str.startswith('NAME')) if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal & f_not_name )
                elif detected_file_type == 'Leatherback or Fusion':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 10000) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )
                elif detected_file_type == 'Cashout 1':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    f_not_gl = (~chunk_df['TRAN_PARTICULAR'].str.startswith('GL')) if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_not_rvsl & f_not_reversal & f_not_gl )
                elif detected_file_type == 'Cashout 2':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_not_ft )
                elif detected_file_type == 'Cashout 3':
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    f_is_nip = chunk_df['TRAN_PARTICULAR'].str.startswith('"NIP"') if 'TRAN_PARTICULAR' in chunk_df.columns else pd.Series(False, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_not_rvsl & f_not_reversal & f_is_nip & f_tp2_not_ft )
                else: # Default case
                    f_amount = (chunk_df['TRANSACTION AMOUNT'] >= 100) if 'TRANSACTION AMOUNT' in chunk_df.columns else pd.Series(True, index=chunk_df.index)
                    combined_filter = ( f_amount_notna & f_amount & f_type_is_c & f_tp2_10_digits & f_not_rvsl & f_not_reversal )

                filtered_chunk_df = chunk_df[combined_filter].copy()

            except KeyError as ke:
                err_msg = f"Filtering error in chunk {i+1}: Column not found - {ke}."
                print(f"ERROR: {err_msg}")
                return None, err_msg, None, None
            except Exception as filter_err:
                err_msg = f"Filtering error in chunk {i+1}: {filter_err}"
                print(f"ERROR: {err_msg} \n{traceback.format_exc()}")
                return None, err_msg, None, None

            chunk_rows_filtered = len(filtered_chunk_df)
            total_rows_filtered += chunk_rows_filtered
            print(f"Chunk {i+1}: Processed {chunk_rows_initial} rows -> Filtered down to {chunk_rows_filtered} rows.")
            sys.stdout.flush()

            if chunk_rows_filtered > 0:
                # --- Calculate Income ---
                if income_rules is None:
                    filtered_chunk_df['Income'] = 0
                else:
                    income_col, warnings_col = zip(*(calculate_income(row, income_rules) for _, row in filtered_chunk_df.iterrows()))
                    filtered_chunk_df['Income'] = income_col
                    unique_warnings.update(w for w in warnings_col if w) # Add non-None warnings

                # --- Aggregate Chunk Results (Include ACCOUNT NAME) ---
                try:
                     # *** UPDATED: Ensure ACCOUNT NAME exists for aggregation ***
                     agg_cols_present = {'ACCOUNT NUMBER', 'ACCOUNT NAME', 'TRANSACTION AMOUNT', 'Income'} <= set(filtered_chunk_df.columns)
                     if agg_cols_present:
                         chunk_aggregated = filtered_chunk_df.groupby('ACCOUNT NUMBER').agg(
                            ACCOUNT_NAME=('ACCOUNT NAME', 'first'), # Get the first account name for the group
                            Total_Transaction_Volume=('ACCOUNT NUMBER', 'size'),
                            Total_Transaction_Value=('TRANSACTION AMOUNT', 'sum'),
                            Total_Income=('Income', 'sum')
                         ).reset_index()
                         all_chunk_aggregated_results.append(chunk_aggregated)
                         # print(f"Chunk {i+1}: Aggregated {len(chunk_aggregated)} partner summaries.") # Optional: Verbose logging
                     else:
                         print(f"WARNING: Chunk {i+1} missing columns needed for aggregation (incl. ACCOUNT NAME). Skipping.")
                except Exception as agg_err:
                    print(f"ERROR during aggregation in chunk {i+1}: {agg_err}\n{traceback.format_exc()}")
                    flash(f"Warning: Error aggregating chunk {i+1}. Results might be incomplete.", "warning")

            chunk_end_time = time.time()
            print(f"Chunk {i+1} processing time: {chunk_end_time - chunk_start_time:.2f} seconds")
            sys.stdout.flush()
            # --- End of chunk loop ---

        # --- Final Aggregation ---
        print("\nCombining results from all chunks...")
        sys.stdout.flush()
        if not all_chunk_aggregated_results:
            print("No results were aggregated from any chunk.")
            # Determine if it was due to filtering or aggregation errors
            if total_rows_processed > 0 and total_rows_filtered == 0:
                 msg = "No transactions passed the filtering criteria."
                 summary = f"Processed {total_rows_processed} rows ({detected_file_type} rules applied), Filtered down to 0 rows."
            elif total_rows_processed > 0 and total_rows_filtered > 0:
                 msg = "Processing completed, but failed to aggregate results."
                 summary = f"Processed {total_rows_processed} rows. Filtered {total_rows_filtered} relevant ({detected_file_type} rules applied). Aggregation failed."
            else: # No rows processed (e.g., empty file)
                 msg = "Input file appears to be empty or could not be read."
                 summary = "Processed 0 rows."

            return pd.DataFrame(columns=['ACCOUNT NUMBER', 'ACCOUNT NAME', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income']), \
                   msg, summary, sorted(list(unique_warnings))


        final_df_concat = pd.concat(all_chunk_aggregated_results, ignore_index=True)

        print("Performing final aggregation across all chunks...")
        sys.stdout.flush()
        # *** UPDATED: Aggregate ACCOUNT_NAME as well using 'first' ***
        final_aggregated_results = final_df_concat.groupby('ACCOUNT NUMBER').agg(
            ACCOUNT_NAME=('ACCOUNT_NAME', 'first'), # Get first name encountered across chunks
            Total_Transaction_Volume=('Total_Transaction_Volume', 'sum'), # Sum the sizes
            Total_Transaction_Value=('Total_Transaction_Value', 'sum'),
            Total_Income=('Total_Income', 'sum')
        ).reset_index()

        # *** ADDED: Reorder columns for output ***
        output_columns = [
            'ACCOUNT NUMBER', 'ACCOUNT NAME', 'Total_Transaction_Volume',
            'Total_Transaction_Value', 'Total_Income'
        ]
        # Ensure all expected columns exist before reordering
        final_aggregated_results = final_aggregated_results.reindex(columns=output_columns)

        print("Final aggregation complete.")
        sys.stdout.flush()

        # --- Prepare Summary ---
        summary_stats = f"Processed {total_rows_processed} rows. Filtered down to {total_rows_filtered} relevant transactions ({detected_file_type} rules applied). Found {len(final_aggregated_results)} unique partners."
        if unique_warnings:
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
        print(f"ERROR during chunk processing: {e}")
        print(traceback.format_exc())
        sys.stdout.flush()
        return None, f"Error processing file in chunks: {e}", None, None


# --- Flask Routes ---

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
        flash('No file part', 'error'); return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No selected file', 'error'); return redirect(request.url)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        saved_file = False
        try:
            print(f"Saving uploaded file: {filename}"); sys.stdout.flush()
            file.save(file_path)
            saved_file = True
            print(f"File saved to {file_path}"); sys.stdout.flush()
            flash('File uploaded. Processing large file (this may take time)...', 'info')

            print("Loading income rules..."); sys.stdout.flush()
            income_rules = load_income_rules()
            if income_rules is None: return redirect(url_for('index')) # Error flashed in helper

            print("Calling process_transactions_chunked..."); sys.stdout.flush()
            results_df, error_msg, summary, warnings = process_transactions_chunked(file_path, income_rules)
            print("Chunked processing finished."); sys.stdout.flush()

            if error_msg:
                flash(error_msg, 'error'); return redirect(url_for('index'))
            if results_df is None:
                 flash("Processing failed to return results.", 'error'); return redirect(url_for('index'))

            # Store results in session for download
            if isinstance(results_df, pd.DataFrame):
                # *** UPDATED: Define session columns including ACCOUNT NAME ***
                session_df_columns = ['ACCOUNT NUMBER', 'ACCOUNT NAME', 'Total_Transaction_Volume', 'Total_Transaction_Value', 'Total_Income']
                # Select only existing columns to avoid errors if a column is missing (e.g., no results)
                cols_to_store = [col for col in session_df_columns if col in results_df.columns]
                session['results_df_json'] = results_df[cols_to_store].to_json(orient='split', date_format='iso')
                session['summary_stats'] = summary
                results_list = results_df.to_dict('records')
            else:
                 flash("Processing returned invalid results format.", 'error'); return redirect(url_for('index'))

            print("Rendering results page."); sys.stdout.flush()
            return render_template('index.html',
                                   results=results_list,
                                   summary_stats=summary)
        except Exception as e:
             print(f"ERROR in upload_file route: {e}\n{traceback.format_exc()}"); sys.stdout.flush()
             flash(f"An unexpected error occurred during processing: {e}", 'error')
             return redirect(url_for('index'))
        finally:
            # Cleanup
            if saved_file and os.path.exists(file_path):
                try: os.remove(file_path); print(f"Cleaned up: {file_path}"); sys.stdout.flush()
                except Exception as ce: print(f"WARN: Cleanup failed {file_path}: {ce}"); sys.stdout.flush()
    else:
        flash('Invalid file type. Allowed: .xlsx, .csv', 'error')
        return redirect(request.url)


@app.route('/download', methods=['POST'])
def download_results():
    """Handles downloading the processed results stored in session."""
    results_json = session.get('results_df_json')
    if not results_json:
        flash('No results available to download.', 'error'); return redirect(url_for('index'))
    try:
        results_df = pd.read_json(results_json, orient='split')
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # *** UPDATED: Rename columns for user-friendly download file ***
            download_cols_rename = {
                'ACCOUNT NUMBER': 'ACCOUNT NUMBER',
                'ACCOUNT_NAME': 'ACCOUNT NAME', # Add mapping
                'Total_Transaction_Volume': 'Total Transaction Volume',
                'Total_Transaction_Value': 'Total Transaction Value',
                'Total_Income': 'Total Income'
            }
            # Select and rename only columns that actually exist in the DataFrame
            cols_to_download = [col for col in download_cols_rename if col in results_df.columns]
            results_df_renamed = results_df[cols_to_download].rename(columns=download_cols_rename)
            # Ensure standard column order in download
            final_download_order = ['ACCOUNT NUMBER', 'ACCOUNT NAME', 'Total Transaction Volume', 'Total Transaction Value', 'Total Income']
            results_df_renamed = results_df_renamed.reindex(columns=final_download_order)

            results_df_renamed.to_excel(writer, index=False, sheet_name='Analysis Results')
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='transaction_analysis_results.xlsx'
        )
    except Exception as e:
        print(f"ERROR generating download file: {e}\n{traceback.format_exc()}"); sys.stdout.flush()
        flash(f"Error generating download file: {e}", 'error')
        return redirect(url_for('index'))

# --- Run Application ---
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
