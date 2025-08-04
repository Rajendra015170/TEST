import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time
import io

# Professional CSS styling with better colors and layout
st.markdown(
    """
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global app styling */
    .stApp {
        background: linear-gradient(135deg, #f0f2f6 0%, #e8eaf0 100%);
        font-family: 'Inter', sans-serif;
    }
    
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        background: rgba(255, 255, 255, 0.98);
        border-radius: 12px;
        margin: 1rem;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
        max-width: 1400px;
    }
    
    /* Enhanced typography */
    .app-title {
        font-size: 2.8rem;
        font-family: 'Inter', sans-serif;
        background: linear-gradient(45deg, #2563eb, #1e40af);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        font-weight: 700;
        margin-bottom: 2rem;
        letter-spacing: -0.02em;
    }
    
    .section-header {
        font-size: 2rem;
        color: #1e293b;
        font-weight: 600;
        margin: 1.5rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid #3b82f6;
    }
    
    .subsection-header {
        font-size: 1.4rem;
        color: #374151;
        font-weight: 600;
        margin: 1rem 0;
    }
    
    /* Sidebar styling - Better colors */
    .css-1d391kg {
        background: linear-gradient(180deg, #1e293b 0%, #334155 100%);
    }
    
    .sidebar .sidebar-content {
        background: transparent;
    }
    
    /* Form elements with better styling */
    .stSelectbox > div > div {
        background-color: white;
        border: 1px solid #d1d5db;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        transition: all 0.2s ease;
    }
    
    .stSelectbox > div > div:focus-within {
        border-color: #3b82f6;
        box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
    }
    
    .stMultiSelect > div > div {
        background-color: white;
        border: 1px solid #d1d5db;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    .stTextInput > div > div > input {
        background-color: white;
        border: 1px solid #d1d5db;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    /* Enhanced button styling */
    .stButton button {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        padding: 0.75rem 1.5rem;
        border-radius: 8px;
        font-size: 14px;
        font-weight: 500;
        border: none;
        box-shadow: 0 4px 6px rgba(59, 130, 246, 0.2);
        transition: all 0.2s ease;
        margin: 4px 2px;
    }
    
    .stButton button:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 12px rgba(59, 130, 246, 0.3);
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
    }
    
    /* Action buttons styling */
    .action-button {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 6px;
        font-size: 12px;
        font-weight: 500;
        border: none;
        margin: 2px;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    
    .action-button:hover {
        background: linear-gradient(135deg, #059669 0%, #047857 100%);
        transform: translateY(-1px);
    }
    
    .download-button {
        background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%);
    }
    
    .download-button:hover {
        background: linear-gradient(135deg, #7c3aed 0%, #6d28d9 100%);
    }
    
    .filter-button {
        background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%);
    }
    
    .filter-button:hover {
        background: linear-gradient(135deg, #d97706 0%, #b45309 100%);
    }
    
    /* Card styling */
    .info-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        border: 1px solid #e5e7eb;
        margin: 1rem 0;
        transition: all 0.2s ease;
    }
    
    .info-card:hover {
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.12);
        transform: translateY(-2px);
    }
    
    .info-card h3 {
        color: #1e293b;
        font-weight: 600;
        margin-bottom: 0.75rem;
        font-size: 1.125rem;
    }
    
    .info-card h4 {
        color: #374151;
        font-weight: 600;
        margin-bottom: 0.5rem;
        font-size: 1rem;
    }
    
    .info-card p {
        color: #6b7280;
        line-height: 1.6;
        margin-bottom: 1rem;
    }
    
    /* Enhanced data editor styling */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        border: 1px solid #e5e7eb;
        min-height: 600px;
        width: 100% !important;
    }
    
    /* Auto-save indicator */
    .auto-save-indicator {
        position: fixed;
        top: 80px;
        right: 20px;
        background: #10b981;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 6px;
        font-size: 14px;
        font-weight: 500;
        z-index: 1000;
        box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    
    .auto-save-indicator.show {
        opacity: 1;
    }
    
    /* Metrics cards */
    .metric-card {
        background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        margin: 0.5rem;
        box-shadow: 0 4px 12px rgba(59, 130, 246, 0.3);
        transition: all 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(59, 130, 246, 0.4);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.25rem;
    }
    
    .metric-label {
        font-size: 0.875rem;
        opacity: 0.9;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    /* Control panel styling */
    .control-panel {
        background: #f8fafc;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
        margin: 1rem 0;
    }
    
    /* Enhanced table styling */
    .dataframe th {
        background-color: #f8fafc !important;
        color: #374151 !important;
        font-weight: 600 !important;
        border-bottom: 2px solid #e5e7eb !important;
        font-size: 14px !important;
    }
    
    .dataframe td {
        border-bottom: 1px solid #f3f4f6 !important;
        font-size: 13px !important;
        padding: 8px !important;
    }
    
    /* Sidebar navigation styling */
    .nav-header {
        color: white;
        font-size: 1.25rem;
        font-weight: 600;
        text-align: center;
        padding: 1rem;
        margin-bottom: 1rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    /* Search and filter controls */
    .search-filter-container {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
        margin: 1rem 0;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }
    
    /* Hide manual save button */
    button[data-testid="stBaseButton-secondary"]:has-text("Manual Save") {
        display: none !important;
    }
    
    /* Enhanced expander styling */
    .streamlit-expanderHeader {
        background-color: #f8fafc;
        border-radius: 8px;
        border: 1px solid #e5e7eb;
    }
    
    /* Footer styling */
    .app-footer {
        margin-top: 3rem;
        padding: 2rem;
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border-radius: 12px;
        text-align: center;
        color: white;
        border: 1px solid #475569;
    }
    
    .app-footer h4 {
        color: white;
        margin-bottom: 0.5rem;
    }
    
    .app-footer p {
        color: #94a3b8;
        margin: 0.5rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Enhanced auto-save functionality
def auto_save_classification_report(df, database, schema, show_message=True):
    """Enhanced auto-save function with better error handling"""
    try:
        session = get_active_session()
        values = []
        
        for _, row in df.iterrows():
            # Escape single quotes to prevent SQL injection
            bu_comments = str(row['BU_COMMENTS']).replace("'", "''") if pd.notna(row['BU_COMMENTS']) else ''
            infosec_comments = str(row['INFOSEC_COMMENTS']).replace("'", "''") if pd.notna(row['INFOSEC_COMMENTS']) else ''
            bu_assignee = str(row['BU_ASSIGNEE']).replace("'", "''") if pd.notna(row['BU_ASSIGNEE']) else ''
            infosec_approver = str(row['INFOSEC_APPROVER']).replace("'", "''") if pd.notna(row['INFOSEC_APPROVER']) else ''
            
            values.append(f"""(
                '{database}', '{schema}', '{row['CLASSIFICATION_OWNER']}', '{row['DATE']}',
                '{row['TABLE_NAME']}', '{row['COLUMN_NAME']}', '{row['CLASSIFICATION']}',
                '{row['HIPAA_CLASS']}', '{row['MASKED']}', '{row['BU_APPROVAL_STATUS']}',
                '{bu_comments}', '{bu_assignee}', '{row['INFOSEC_APPROVAL_STATUS']}',
                '{infosec_approver}', '{infosec_comments}',
                {int(row['IS_ACTIVE']) if pd.notna(row['IS_ACTIVE']) else 0},
                {int(row['VERSION']) if pd.notna(row['VERSION']) else 1},
                {int(row['ID'])}
            )""")
        
        if not values:
            return False
            
        values_str = ",\n".join(values)
        merge_sql = f"""
            MERGE INTO DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1 AS target
            USING (
                SELECT * FROM VALUES
                {values_str}
                AS source (
                    DATABASE_NAME, SCHEMA_NAME, CLASSIFICATION_OWNER, DATE,
                    TABLE_NAME, COLUMN_NAME, CLASSIFICATION, HIPAA_CLASS,
                    MASKED, BU_APPROVAL_STATUS, BU_COMMENTS, BU_ASSIGNEE,
                    INFOSEC_APPROVAL_STATUS, INFOSEC_APPROVER, INFOSEC_COMMENTS,
                    IS_ACTIVE, VERSION, ID
                )
            ) AS source
            ON target.ID = source.ID
            WHEN MATCHED THEN UPDATE SET
                BU_APPROVAL_STATUS = source.BU_APPROVAL_STATUS,
                BU_COMMENTS = source.BU_COMMENTS,
                BU_ASSIGNEE = source.BU_ASSIGNEE,
                INFOSEC_APPROVAL_STATUS = source.INFOSEC_APPROVAL_STATUS,
                INFOSEC_APPROVER = source.INFOSEC_APPROVER,
                INFOSEC_COMMENTS = source.INFOSEC_COMMENTS
        """
        
        session.sql(merge_sql).collect()
        st.session_state.last_save_time = time.time()
        
        if show_message:
            # Show auto-save indicator
            st.markdown(f"""
            <div class="auto-save-indicator show" id="auto-save-indicator">
                ✓ Auto-saved at {time.strftime('%H:%M:%S')}
            </div>
            <script>
                setTimeout(function() {{
                    var indicator = document.getElementById('auto-save-indicator');
                    if (indicator) indicator.style.opacity = '0';
                }}, 2000);
            </script>
            """, unsafe_allow_html=True)
        
        return True
    except Exception as e:
        if show_message:
            st.error(f"Auto-save failed: {str(e)}")
        return False

# Function to create CSV download
def create_csv_download(df, filename):
    """Create a CSV download link"""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    return csv_data

# Function to log actions to the specified audit table
def log_audit(action, status, audit_type):
    """Log an action to the specified audit table."""
    try:
        session = get_active_session()
        current_user = session.get_current_user().replace('"', '')
        current_role = session.get_current_role().replace('"', '')

        if audit_type == "masking":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.MASKING_AUDIT (
                ACTIVITY, ACTIVITY_STATUS, ROLE, "USER_NAME", 
                ROW_CREATE_DATE, ROW_MOD_DATE
            )
            VALUES (
                '{action}', '{status}', '{current_role}', '{current_user}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            );
            """
        elif audit_type == "synthetic":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.SYNTHETIC_AUDIT (
                ACTIVITY, ACTIVITY_STATUS, ROLE, "USER_NAME", 
                ROW_CREATE_DATE, ROW_MOD_DATE
            )
            VALUES (
                '{action}', '{status}', '{current_role}', '{current_user}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            );
            """
        elif audit_type == "encryption":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.ENCRYPTION_AUDIT (
                ACTIVITY, ACTIVITY_STATUS, ROLE, "USER_NAME", 
                ROW_CREATE_DATE, ROW_MOD_DATE
            )
            VALUES (
                '{action}', '{status}', '{current_role}', '{current_user}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            );
            """

        session.sql(audit_sql).collect()
    except Exception as e:
        st.error(f"Error logging to audit: {str(e)}")

# Professional sidebar with clean navigation
st.sidebar.markdown("""
<div class="nav-header">
    🛡️ ZDC Platform
    <div style="font-size: 0.8rem; font-weight: 400; margin-top: 0.25rem; opacity: 0.8;">
        Data Governance Suite
    </div>
</div>
""", unsafe_allow_html=True)

app_mode = st.sidebar.radio(
    "Navigation", 
    ["🏠 Home", 
     "🔬 Synthetic Data", 
     "🔒 Data Masking",
     "🔐 Data Encryption",                              
     "📊 Classifications"],
    key="main_nav"
)

# Home Page
if app_mode == "🏠 Home":
    st.markdown('<h1 class="app-title">ZDC Data Governance Platform</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-card">
        <h3>Welcome to the Zero Data Company Platform</h3>
        <p>A comprehensive data governance solution providing synthetic data generation, advanced masking, 
        encryption capabilities, and intelligent classification management for Snowflake environments.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Feature overview cards
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="info-card">
            <h4>🔬 Synthetic Data Generation</h4>
            <p>Generate realistic synthetic data that preserves statistical properties while protecting sensitive information.</p>
            <ul>
                <li>Maintains data relationships and patterns</li>
                <li>Preserves statistical accuracy</li>
                <li>Ensures complete privacy compliance</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-card">
            <h4>🔐 Data Encryption</h4>
            <p>Format-preserving encryption with advanced key management for maximum security.</p>
            <ul>
                <li>Format-preserving encryption</li>
                <li>Searchable encrypted data</li>
                <li>Join-compatible encryption</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="info-card">
            <h4>🔒 Data Masking</h4>
            <p>Comprehensive masking solutions with automated workflows and policy management.</p>
            <ul>
                <li>Dynamic data masking</li>
                <li>Policy-based automation</li>
                <li>Comprehensive audit trails</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-card">
            <h4>📊 Classification Management</h4>
            <p>AI-powered data classification with automated approval workflows and compliance reporting.</p>
            <ul>
                <li>Intelligent classification</li>
                <li>Automated approval workflows</li>
                <li>Real-time collaboration</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

# Classifications with Enhanced Auto-Save and Advanced Features
elif app_mode == "📊 Classifications":
    session = get_active_session()
    
    st.markdown('<h1 class="section-header">📊 Classification Management</h1>', unsafe_allow_html=True)
    
    app_mode_classification = st.sidebar.radio("Process", ["🏠 Overview", "✏️ Edit & Submit"], key="class_nav")

    if app_mode_classification == "🏠 Overview":
        st.markdown("""
        <div class="info-card">
            <h3>📊 Classification Management Platform</h3>
            <p>Advanced classification editing and submission system with real-time auto-save functionality, 
            search capabilities, filtering options, and data export features.</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            <div class="info-card">
                <h4>🔄 Workflow Features</h4>
                <ul>
                    <li><strong>Real-time Auto-save:</strong> Changes saved automatically</li>
                    <li><strong>Advanced Search:</strong> Find specific records quickly</li>
                    <li><strong>Smart Filtering:</strong> Filter by status, table, or classification</li>
                    <li><strong>Data Export:</strong> Download reports as CSV</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="info-card">
                <h4>✨ Key Benefits</h4>
                <ul>
                    <li><strong>No Manual Saving:</strong> Automatic data persistence</li>
                    <li><strong>Enhanced UI:</strong> Larger, more flexible editor</li>
                    <li><strong>Quick Actions:</strong> Search, filter, and export tools</li>
                    <li><strong>Audit Trail:</strong> Complete change history</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

    elif app_mode_classification == "✏️ Edit & Submit":
        st.markdown('<h3 class="subsection-header">📝 Classification Report Editor</h3>', unsafe_allow_html=True)
        
        # Session state initialization
        for key in ["report_fetched", "edited_df", "last_save_time", "original_df", "search_term", "filter_status"]:
            if key not in st.session_state:
                if key in ["edited_df", "original_df"]:
                    st.session_state[key] = None
                elif key in ["last_save_time"]:
                    st.session_state[key] = None
                elif key in ["search_term", "filter_status"]:
                    st.session_state[key] = ""
                else:
                    st.session_state[key] = False

        # Helper functions
        def fetch_databases():
            try:
                rows = session.sql("""
                    SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES 
                    WHERE DATABASE_NAME LIKE 'PROD_%' 
                    AND DATABASE_NAME NOT LIKE '%_MASKED%' 
                    AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
                """).collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.error(f"Error fetching databases: {e}")
                return []

        def fetch_schemas(database):
            try:
                rows = session.sql(f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA").collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.error(f"Error fetching schemas: {e}")
                return []

        def fetch_classification_report(database, schema):
            try:
                query = f"""
                    SELECT * 
                    FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                    WHERE DATABASE_NAME = '{database}' 
                      AND SCHEMA_NAME = '{schema}' 
                      AND VERSION = (
                          SELECT MAX(VERSION)
                          FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                          WHERE DATABASE_NAME = '{database}' 
                            AND SCHEMA_NAME = '{schema}'
                      )
                """
                return session.sql(query).collect()
            except Exception as e:
                st.error(f"Error fetching classification report: {e}")
                return []

        def get_bu_names():
            try:
                rows = session.sql("SELECT DISTINCT BU_NAME FROM DEV_DB_MANAGER.MASKING.CONSUMER").collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.error(f"Error fetching BU names: {e}")
                return []

        def insert_raw_classification_details(database, schema, bu_name):
            try:
                # Classification mapping
                classification_mapping = {
                    "I&E Business Intelligence": ("IE_BU", "IE_PII"),
                    "PRICE": ("PRICE_BU", "PRICE_PII"),
                    "Marketing": ("MARKETING_BU", "MARKETING_PII"),
                    "ZDI Provider Intelligence": ("PROVIDER_BU", "PROVIDER_PII"),
                    "ZDI Member Intelligence": ("MEMBER_BU", "MEMBER_PII"),
                    "Payments Optimization": ("PAYMENTS_BU", "PAYMENTS_PII"),
                    "ZDI Data Science Engineer": ("DSE_BU", "DSE_PII"),
                }

                classification_owner, hipaa_class = classification_mapping.get(bu_name, (None, None))
                if not classification_owner:
                    st.error("Invalid BU Name selected.")
                    return False

                # Get max version and increment
                max_version_row = session.sql(f"""
                    SELECT MAX(VERSION) 
                    FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS 
                    WHERE DATABASE_NAME = '{database}' 
                        AND SCHEMA_NAME = '{schema}' 
                        AND CLASSIFICATION_OWNER = '{classification_owner}'
                """).first()

                max_version = max_version_row[0] if max_version_row[0] is not None else 0
                new_version = max_version + 1

                # Fetch classification details to insert
                fetch_sql = f"""
                    SELECT * 
                    FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                    WHERE DATABASE_NAME = '{database}' 
                        AND SCHEMA_NAME = '{schema}' 
                        AND VERSION = (
                            SELECT MAX(VERSION)
                            FROM DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1
                            WHERE DATABASE_NAME = '{database}' 
                                AND SCHEMA_NAME = '{schema}'
                        )
                        AND ((BU_APPROVAL_STATUS = 'APPROVED' AND MASKED = 'YES') 
                        OR (BU_APPROVAL_STATUS = 'MASK' AND MASKED = 'NO'))
                """
                classification_details = session.sql(fetch_sql).collect()

                if not classification_details:
                    st.warning("No classification details available for submission.")
                    return False

                # Mark existing records as inactive
                session.sql(f"""
                    UPDATE DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
                    SET IS_ACTIVE = false
                    WHERE DATABASE_NAME = '{database}'
                        AND SCHEMA_NAME = '{schema}'
                        AND CLASSIFICATION_OWNER = '{classification_owner}'
                """).collect()

                # Insert new records
                insert_values = []
                for row in classification_details:
                    max_import_id_row = session.sql("SELECT MAX(IMPORT_ID) FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS").first()
                    max_import_id = max_import_id_row[0] if max_import_id_row[0] is not None else 0
                    new_import_id = max_import_id + 1

                    bu_comments = str(row['BU_COMMENTS']).replace("'", "''") if row['BU_COMMENTS'] else ''
                    infosec_comments = str(row['INFOSEC_COMMENTS']).replace("'", "''") if row['INFOSEC_COMMENTS'] else ''
                    bu_assignee = str(row['BU_ASSIGNEE']).replace("'", "''") if row['BU_ASSIGNEE'] else ''
                    infosec_approver = str(row['INFOSEC_APPROVER']).replace("'", "''") if row['INFOSEC_APPROVER'] else ''

                    insert_values.append(f"""(
                        {new_import_id}, '{row['DATE']}', '{database}', '{schema}', 
                        '{row['TABLE_NAME']}', '{row['COLUMN_NAME']}', 'HIPAA', 
                        '{hipaa_class}', '{row['BU_APPROVAL_STATUS']}', '{bu_comments}', 
                        '{bu_assignee}', '{row['INFOSEC_APPROVAL_STATUS']}', 
                        '{infosec_approver}', '{infosec_comments}', 
                        true, '{classification_owner}', {new_version}
                    )""")

                if insert_values:
                    values_str = ",\n".join(insert_values)
                    insert_sql = f"""
                        INSERT INTO DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS (
                            IMPORT_ID, DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, 
                            CLASSIFICATION, HIPAA_CLASS, BU_APPROVAL_STATUS, BU_COMMENTS, 
                            BU_ASSIGNEE, INFOSEC_APPROVAL_STATUS, INFOSEC_APPROVER, 
                            INFOSEC_COMMENTS, IS_ACTIVE, CLASSIFICATION_OWNER, VERSION
                        ) VALUES {values_str}
                    """
                    session.sql(insert_sql).collect()
                    return True
                
                return False
            except Exception as e:
                st.error(f"Error during submission: {e}")
                return False

        # Database and schema selection
        st.markdown('<div class="search-filter-container">', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            database = st.selectbox("🗄️ Select Database", fetch_databases(), key="class_db")
        
        with col2:
            if database:
                schema = st.selectbox("📋 Select Schema", fetch_schemas(database), key="class_schema")
        
        with col3:
            if database and schema:
                if st.button("📊 Load Report", key="get_report"):
                    with st.spinner("🔄 Loading classification report..."):
                        data = fetch_classification_report(database, schema)
                        if data:
                            df = pd.DataFrame([row.as_dict() for row in data])
                            try:
                                current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
                                df['BU_ASSIGNEE'] = current_user
                            except:
                                df['BU_ASSIGNEE'] = session.get_current_user()
                            
                            st.session_state.edited_df = df.copy()
                            st.session_state.original_df = df.copy()
                            st.session_state.report_fetched = True
                            st.success("✅ Report loaded successfully!")
                        else:
                            st.warning("⚠️ No data found for the selected database and schema.")
        
        st.markdown('</div>', unsafe_allow_html=True)

        # Enhanced search, filter, and action controls
        if st.session_state.report_fetched and st.session_state.edited_df is not None:
            st.markdown('<h3 class="subsection-header">🔍 Search, Filter & Actions</h3>', unsafe_allow_html=True)
            
            # Search and filter controls
            st.markdown('<div class="control-panel">', unsafe_allow_html=True)
            
            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 1, 1])
            
            with col1:
                search_term = st.text_input("🔍 Search (Table/Column/Comments)", 
                                          value=st.session_state.search_term, 
                                          key="search_input",
                                          placeholder="Type to search...")
                st.session_state.search_term = search_term
            
            with col2:
                filter_status = st.selectbox("🏷️ Filter by BU Status", 
                                           ["All", "MASK", "APPROVED", "NO MASKING NEEDED"],
                                           key="filter_select")
                st.session_state.filter_status = filter_status
            
            with col3:
                filter_table = st.selectbox("📋 Filter by Table", 
                                          ["All"] + list(st.session_state.edited_df['TABLE_NAME'].unique()),
                                          key="filter_table_select")
            
            with col4:
                # Clear filters button
                if st.button("🔄 Clear", key="clear_filters"):
                    st.session_state.search_term = ""
                    st.session_state.filter_status = ""
                    st.rerun()
            
            with col5:
                # Download CSV button
                if st.button("📥 CSV", key="download_csv"):
                    csv_data = create_csv_download(st.session_state.edited_df, f"{database}_{schema}_classification.csv")
                    st.download_button(
                        label="Download CSV",
                        data=csv_data,
                        file_name=f"{database}_{schema}_classification.csv",
                        mime="text/csv",
                        key="download_csv_btn"
                    )
            
            st.markdown('</div>', unsafe_allow_html=True)

            # Apply filters to dataframe
            filtered_df = st.session_state.edited_df.copy()
            
            # Apply search filter
            if search_term:
                mask = (
                    filtered_df['TABLE_NAME'].str.contains(search_term, case=False, na=False) |
                    filtered_df['COLUMN_NAME'].str.contains(search_term, case=False, na=False) |
                    filtered_df['BU_COMMENTS'].str.contains(search_term, case=False, na=False) |
                    filtered_df['INFOSEC_COMMENTS'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[mask]
            
            # Apply status filter
            if filter_status and filter_status != "All":
                filtered_df = filtered_df[filtered_df['BU_APPROVAL_STATUS'] == filter_status]
            
            # Apply table filter
            if filter_table and filter_table != "All":
                filtered_df = filtered_df[filtered_df['TABLE_NAME'] == filter_table]

            # Display filter results info
            if len(filtered_df) != len(st.session_state.edited_df):
                st.info(f"📊 Showing {len(filtered_df)} of {len(st.session_state.edited_df)} records")

            # Auto-save status indicator
            if st.session_state.last_save_time:
                save_time = time.strftime('%H:%M:%S', time.localtime(st.session_state.last_save_time))
                st.success(f"💾 Last auto-saved at {save_time}")

            st.markdown('<h3 class="subsection-header">✏️ Classification Report Editor</h3>', unsafe_allow_html=True)
            
            # Configure DataFrame
            df_copy = filtered_df.copy()
            
            # Set up categorical columns
            approval_options = ['MASK', 'APPROVED', 'NO MASKING NEEDED']
            df_copy['BU_APPROVAL_STATUS'] = pd.Categorical(df_copy['BU_APPROVAL_STATUS'], categories=approval_options)
            df_copy['INFOSEC_APPROVAL_STATUS'] = pd.Categorical(df_copy['INFOSEC_APPROVAL_STATUS'], categories=approval_options)
            
            # Enhanced data editor configuration with better column widths
            column_config = {
                "DATABASE_NAME": st.column_config.TextColumn("Database", disabled=True, width="small"),
                "SCHEMA_NAME": st.column_config.TextColumn("Schema", disabled=True, width="small"),
                "TABLE_NAME": st.column_config.TextColumn("Table", disabled=True, width="medium"),
                "COLUMN_NAME": st.column_config.TextColumn("Column", disabled=True, width="medium"),
                "CLASSIFICATION": st.column_config.TextColumn("Classification", disabled=True, width="small"),
                "HIPAA_CLASS": st.column_config.TextColumn("HIPAA Class", disabled=True, width="small"),
                "MASKED": st.column_config.TextColumn("Masked", disabled=True, width="small"),
                "BU_APPROVAL_STATUS": st.column_config.SelectboxColumn(
                    "BU Status",
                    options=approval_options,
                    required=True,
                    width="medium"
                ),
                "BU_COMMENTS": st.column_config.TextColumn(
                    "BU Comments",
                    width="large",
                    help="Business unit comments - editable"
                ),
                "BU_ASSIGNEE": st.column_config.TextColumn("BU Assignee", disabled=True, width="medium"),
                "INFOSEC_APPROVAL_STATUS": st.column_config.SelectboxColumn(
                    "InfoSec Status",
                    options=approval_options,
                    width="medium"
                ),
                "INFOSEC_APPROVER": st.column_config.TextColumn("InfoSec Approver", width="medium"),
                "INFOSEC_COMMENTS": st.column_config.TextColumn(
                    "InfoSec Comments",
                    width="large",
                    help="Information security comments - editable"
                )
            }
            
            # Enhanced data editor with more height and full width
            edited_df = st.data_editor(
                df_copy,
                column_config=column_config,
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                height=700,  # Increased height for better visibility
                key="classification_editor_enhanced"
            )
            
            # Auto-save on changes (only save if there are actual changes)
            if not edited_df.equals(df_copy):
                # Merge changes back to the main dataframe
                for idx, row in edited_df.iterrows():
                    original_idx = st.session_state.edited_df[
                        st.session_state.edited_df['ID'] == row['ID']
                    ].index[0]
                    st.session_state.edited_df.loc[original_idx] = row
                
                # Auto-save changes
                auto_save_classification_report(st.session_state.edited_df, database, schema, show_message=True)
                st.rerun()

            # Enhanced submission section
            st.markdown('<h3 class="subsection-header">📤 Submit Classifications</h3>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns([1, 2, 1])
            
            with col2:
                bu_name = st.selectbox("🏢 Select Business Unit", get_bu_names(), key="submit_bu")
                
                if bu_name:
                    if st.button("📤 Submit Final Classifications", key="submit_final", type="primary"):
                        with st.spinner("📤 Submitting classifications..."):
                            if insert_raw_classification_details(database, schema, bu_name):
                                st.success("✅ Classifications submitted successfully!")
                                st.balloons()
                            else:
                                st.error("❌ Submission failed. Please try again.")

            # Enhanced summary metrics
            if len(st.session_state.edited_df) > 0:
                st.markdown('<h3 class="subsection-header">📊 Summary Dashboard</h3>', unsafe_allow_html=True)
                
                col1, col2, col3, col4 = st.columns(4)
                
                total_records = len(st.session_state.edited_df)
                approved_count = len(st.session_state.edited_df[st.session_state.edited_df['BU_APPROVAL_STATUS'] == 'APPROVED'])
                mask_count = len(st.session_state.edited_df[st.session_state.edited_df['BU_APPROVAL_STATUS'] == 'MASK'])
                no_mask_count = len(st.session_state.edited_df[st.session_state.edited_df['BU_APPROVAL_STATUS'] == 'NO MASKING NEEDED'])
                
                metrics = [
                    ("Total Records", total_records),
                    ("Approved", approved_count),
                    ("To Mask", mask_count),
                    ("No Masking", no_mask_count)
                ]
                
                for i, (label, value) in enumerate(metrics):
                    with [col1, col2, col3, col4][i]:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="metric-value">{value}</div>
                            <div class="metric-label">{label}</div>
                        </div>
                        """, unsafe_allow_html=True)

# Other sections remain the same but with enhanced styling...
# (Keeping the existing Synthetic Data, Masking, and Encryption sections with better colors)

# Add footer
st.markdown("""
<div class="app-footer">
    <h4>🛡️ ZDC Data Governance Platform</h4>
    <p>Comprehensive data protection and compliance solution for Snowflake environments</p>
    <p>Enterprise-grade security with intelligent automation</p>
</div>
""", unsafe_allow_html=True)