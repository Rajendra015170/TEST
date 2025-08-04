import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import time

# Custom CSS for styling with improved colors and responsiveness
st.markdown(
    """
    <style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #2c3e50 0%, #34495e 100%);
    }
    
    /* Main content area - removed padding and margins for more space */
    .main .block-container {
        background-color: rgba(255, 255, 255, 0.95);
        border-radius: 15px;
        padding: 1rem;
        margin: 0.5rem;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
        max-width: 100%;
        width: 100%;
    }

    .font {
        font-size: 36px;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        color: #2c3e50;
        text-transform: uppercase;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1.5rem;
    }

    /* Enhanced sidebar styling */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #34495e 0%, #2c3e50 100%);
        color: #ecf0f1;
        border-radius: 10px;
    }

    /* Button styling with modern gradient */
    .stButton button {
        background: linear-gradient(45deg, #3498db, #2980b9);
        color: white;
        padding: 12px 24px;
        border-radius: 8px;
        font-size: 16px;
        font-weight: 600;
        margin: 8px 0;
        border: none;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(52, 152, 219, 0.3);
    }

    .stButton button:hover {
        background: linear-gradient(45deg, #2980b9, #1a6b96);
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(52, 152, 219, 0.4);
    }

    /* Enhanced headers */
    .stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
        color: #2c3e50;
        font-weight: 600;
        margin-bottom: 1rem;
    }

    .stMarkdown h2 {
        border-bottom: 3px solid #3498db;
        padding-bottom: 0.5rem;
    }

    /* Paragraph styling */
    .stMarkdown p {
        color: #34495e;
        line-height: 1.6;
        font-size: 16px;
    }

    /* Selectbox styling */
    .stSelectbox > div > div {
        background-color: #f8f9fa;
        border-radius: 8px;
        border: 2px solid #e9ecef;
    }

    /* Success/Error message styling */
    .stAlert {
        border-radius: 10px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
    }

    /* Enhanced Data editor styling for maximum width */
    .stDataFrame {
        background-color: white;
        border-radius: 10px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        width: 100% !important;
        max-width: 100% !important;
    }

    /* Custom styling for data editor container - full width */
    .data-editor-container {
        height: 700px !important;
        width: 100% !important;
        max-width: 100% !important;
        overflow: auto;
        background-color: white;
        border-radius: 10px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        margin: 1rem 0;
        padding: 0;
    }

    /* Remove any constraints on the data editor */
    .stDataFrame > div {
        width: 100% !important;
        max-width: 100% !important;
    }

    /* Ensure the dataframe takes full width */
    div[data-testid="stDataFrame"] {
        width: 100% !important;
        max-width: 100% !important;
    }

    div[data-testid="stDataFrame"] > div {
        width: 100% !important;
        max-width: 100% !important;
    }

    /* Override any column width restrictions */
    .element-container {
        width: 100% !important;
        max-width: 100% !important;
    }

    /* Make sure the main content uses full width */
    .block-container {
        padding-left: 1rem !important;
        padding-right: 1rem !important;
        max-width: 100% !important;
        width: 100% !important;
    }

    /* Auto-save indicator */
    .auto-save-indicator {
        position: fixed;
        top: 20px;
        right: 20px;
        background: linear-gradient(45deg, #27ae60, #2ecc71);
        color: white;
        padding: 8px 16px;
        border-radius: 20px;
        font-size: 14px;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(39, 174, 96, 0.3);
        z-index: 1000;
        opacity: 0;
        transition: opacity 0.3s ease;
    }

    .auto-save-indicator.show {
        opacity: 1;
    }

    /* Force full width on all streamlit containers */
    .css-1kyxreq {
        max-width: 100% !important;
        width: 100% !important;
    }

    .css-12oz5g7 {
        max-width: 100% !important;
        width: 100% !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Auto-save functionality for session state
def auto_save_to_session(key, value):
    """Auto-save data to session state"""
    if key not in st.session_state or st.session_state[key] != value:
        st.session_state[key] = value
        return True
    return False

# Function to show auto-save indicator
def show_auto_save_indicator():
    """Show auto-save success indicator"""
    st.markdown("""
    <div class="auto-save-indicator">
        💾 Auto-saved successfully
    </div>
    <script>
    setTimeout(function() {
        const indicator = document.querySelector('.auto-save-indicator');
        if (indicator) {
            indicator.style.opacity = '0';
            setTimeout(() => indicator.remove(), 300);
        }
    }, 2000);
    </script>
    """, unsafe_allow_html=True)

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
                ACTIVITY, 
                ACTIVITY_STATUS, 
                ROLE, 
                "USER_NAME", 
                ROW_CREATE_DATE, 
                ROW_MOD_DATE
            )
            VALUES (
                '{action}',
                '{status}', 
                '{current_role}',
                '{current_user}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
            """
        elif audit_type == "synthetic":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.SYNTHETIC_AUDIT (
                ACTIVITY, 
                ACTIVITY_STATUS, 
                ROLE, 
                "USER_NAME", 
                ROW_CREATE_DATE, 
                ROW_MOD_DATE
            )
            VALUES (
                '{action}',
                '{status}', 
                '{current_role}',
                '{current_user}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
            """
        elif audit_type == "encryption":
            audit_sql = f"""
            INSERT INTO PROD_DB_MANAGER.PUBLIC.ENCRYPTION_AUDIT (
                ACTIVITY, 
                ACTIVITY_STATUS, 
                ROLE, 
                "USER_NAME", 
                ROW_CREATE_DATE, 
                ROW_MOD_DATE
            )
            VALUES (
                '{action}',
                '{status}', 
                '{current_role}',
                '{current_user}',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            );
            """

        session.sql(audit_sql).collect()
    except Exception as e:
        st.error(f"❌ Error logging to audit: {str(e)}", icon="🚨")

# Enhanced sidebar with modern design
st.sidebar.markdown("""
<div style='text-align: center; padding: 2rem 1rem; background: rgba(255,255,255,0.1); border-radius: 15px; margin-bottom: 2rem;'>
    <h1 style='color: white; margin: 0; font-size: 2rem; font-weight: 700;'>🛡️ ZDC</h1>
    <p style='color: rgba(255,255,255,0.9); margin: 0.5rem 0 0 0; font-size: 1rem; font-weight: 300;'>Data Governance Platform</p>
</div>
""", unsafe_allow_html=True)

app_mode = st.sidebar.radio(
    "🚀 Select a Function:", 
    ["🏠 Home", 
     "🔬 Synthetic Data Generation", 
     "🔒 Snowflake Masking",
     "🔐 Snowflake Encryption",                              
     "📊 Classifications"],
    key="main_nav"
)

# Home Page with enhanced design
if app_mode == "🏠 Home":
    st.markdown('<h1 class="font">Welcome to the ZDC Platform</h1>', unsafe_allow_html=True)
    
    st.markdown("""
    <div style="text-align: center; margin: 2rem 0; padding: 2rem; background: linear-gradient(135deg, #667eea, #764ba2); border-radius: 20px; color: white;">
        <h2 style="margin: 0; font-weight: 300;">Enterprise Data Governance & Protection</h2>
        <p style="margin: 1rem 0 0 0; opacity: 0.9; font-size: 1.1rem;">Secure, compliant, and intelligent data management for modern enterprises</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Feature cards with better spacing
    col1, col2 = st.columns(2, gap="large")
    
    with col1:
        st.markdown("""
        <div class="info-card">
            <h3>🔬 Synthetic Data Generation</h3>
            <p>Generate synthetic data that maintains statistical properties while protecting sensitive information. Perfect for development and testing environments.</p>
            <ul style="margin-top: 1rem;">
                <li>Preserve data relationships</li>
                <li>Maintain statistical accuracy</li>
                <li>Ensure privacy compliance</li>
                <li>Automated quality validation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-card">
            <h3>🔐 Data Encryption</h3>
            <p>Advanced encryption capabilities for protecting sensitive data at rest and in transit with format-preserving technology.</p>
            <ul style="margin-top: 1rem;">
                <li>Format-preserving encryption</li>
                <li>Enterprise key management</li>
                <li>Secure data processing</li>
                <li>Compliance automation</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="info-card">
            <h3>🔒 Data Masking</h3>
            <p>Comprehensive data masking solutions for Snowflake environments with automated workflows and policy management.</p>
            <ul style="margin-top: 1rem;">
                <li>Dynamic data masking</li>
                <li>Policy-based automation</li>
                <li>Audit and compliance</li>
                <li>Real-time monitoring</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-card">
            <h3>📊 Classification Management</h3>
            <p>Intelligent data classification and governance with automated approval workflows and AI-powered insights.</p>
            <ul style="margin-top: 1rem;">
                <li>AI-powered classification</li>
                <li>Approval workflows</li>
                <li>Compliance reporting</li>
                <li>Real-time collaboration</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

# Synthetic Data Generation with enhanced UI
elif app_mode == "🔬 Synthetic Data Generation":
    st.markdown('<h1 class="section-header">🔬 Synthetic Data Generation</h1>', unsafe_allow_html=True)
    
    st.sidebar.markdown("### 🔬 Synthetic Data Process")
    data_gen_mode = st.sidebar.radio("Select Process:", ["🏠 Overview", "⚙️ Data Generation"], key="synthetic_nav")

    if data_gen_mode == "🏠 Overview":
        st.markdown("""
        <div class="info-card">
            <h3>📋 Process Overview</h3>
            <p>Generate synthetic data that maintains the structure and statistical properties of your source data without compromising sensitive information.</p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns(2, gap="large")
        
        with col1:
            st.markdown("""
            <div class="info-card">
                <h4>✅ Requirements</h4>
                <ul>
                    <li>Minimum 20 distinct rows per table</li>
                    <li>Maximum 100 columns per table</li>
                    <li>Maximum 14 million rows per table</li>
                    <li>Valid data types and constraints</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="info-card">
                <h4>🚫 Limitations</h4>
                <ul>
                    <li>External tables not supported</li>
                    <li>Apache Iceberg tables not supported</li>
                    <li>Streams not supported</li>
                    <li>Temporary tables excluded</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

    elif data_gen_mode == "⚙️ Data Generation":
        session = get_active_session()

        # Functions for data generation (keeping existing logic but with enhanced UI)
        def get_databases(env_prefix=None):
            if env_prefix:
                db_query = f"""
                SELECT DATABASE_NAME 
                FROM INFORMATION_SCHEMA.DATABASES 
                WHERE DATABASE_NAME LIKE '{env_prefix}%' AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
                """
            else:
                db_query = """
                SELECT DATABASE_NAME 
                FROM INFORMATION_SCHEMA.DATABASES
                """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        def get_schemas(database):
            schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        def get_tables_for_schema(database, schema):
            table_query = f"""
            SELECT TABLE_NAME 
            FROM {database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE = 'BASE TABLE'
            """
            rows = session.sql(table_query).collect()
            return [row[0] for row in rows]

        def get_columns_for_table(database, schema, table):
            columns_query = f"""
            SELECT COLUMN_NAME 
            FROM {database}.INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """
            rows = session.sql(columns_query).collect()
            return [row[0] for row in rows]

        def has_valid_data(database, schema, table):
            try:
                column_query = f"""
                SELECT COLUMN_NAME 
                FROM {database}.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' 
                ORDER BY ORDINAL_POSITION LIMIT 1
                """
                columns = session.sql(column_query).collect()

                if not columns:
                    return False
                
                first_column = columns[0][0]

                check_query = f"""
                SELECT COUNT(*) 
                FROM {database}.{schema}.{table} 
                WHERE {first_column} IS NOT NULL
                """
                result = session.sql(check_query).collect()
                return result[0][0] > 1  

            except Exception as e:
                st.error(f"Error checking valid data for {database}.{schema}.{table}: {e}")
                return False

        # Enhanced form layout
        st.markdown('<h3 class="subsection-header">📊 Configuration</h3>', unsafe_allow_html=True)
        
        with st.container():
            col1, col2, col3, col4 = st.columns(4, gap="large")
            
            with col1:
                env = st.selectbox("🌍 Environment", ["DEV", "QA", "UAT", "PROD"], key="env_select")
            
            with col2:
                database_list_source = get_databases(env)
                selected_source_database = st.selectbox("🗄️ Source Database", database_list_source, key="source_database")
            
            with col3:
                if selected_source_database:
                    source_schema_list = get_schemas(selected_source_database)
                    selected_source_schema = st.selectbox("📋 Source Schema", source_schema_list, key="source_schema")
            
            with col4:
                database_list_target = (
                    get_databases("DEV") +
                    get_databases("QA") +
                    get_databases("UAT") +
                    get_databases("PROD")
                )
                selected_target_database = st.selectbox("🎯 Target Database", database_list_target, key="target_database")

        # Table selection with enhanced UI
        if selected_source_schema:
            st.markdown('<h3 class="subsection-header">📋 Table Selection</h3>', unsafe_allow_html=True)
            
            col1, col2 = st.columns([3, 1], gap="large")
            
            with col1:
                source_table_list = get_tables_for_schema(selected_source_database, selected_source_schema)
                
                if "selected_tables" not in st.session_state:
                    st.session_state.selected_tables = []
                if "join_keys" not in st.session_state:
                    st.session_state.join_keys = {}

                selected_tables = st.multiselect(
                    "📊 Select Source Tables", 
                    options=source_table_list, 
                    key="source_tables", 
                    default=st.session_state.selected_tables
                )
                
                st.session_state.selected_tables = selected_tables
            
            with col2:
                if selected_target_database:
                    target_schema_list = get_schemas(selected_target_database)
                    selected_target_schema = st.selectbox("🎯 Target Schema", target_schema_list, key="target_schema")

        # Join keys configuration with enhanced layout
        if selected_tables:
            st.markdown('<h3 class="subsection-header">🔗 Join Keys Configuration</h3>', unsafe_allow_html=True)
            
            for i, table in enumerate(selected_tables):
                with st.expander(f"📋 Configure Join Keys for {table}", expanded=i==0):
                    if table not in st.session_state.join_keys:
                        st.session_state.join_keys[table] = []

                    columns = get_columns_for_table(selected_source_database, selected_source_schema, table)
                    default_join_keys = [key for key in st.session_state.join_keys[table] if key in columns]

                    join_keys = st.multiselect(
                        f"🔗 Join Keys for {table}", 
                        options=columns, 
                        default=default_join_keys,
                        key=f"join_keys_{table}"
                    )
                    
                    st.session_state.join_keys[table] = join_keys

        # Action buttons with enhanced styling
        if selected_tables and selected_target_schema:
            st.markdown('<h3 class="subsection-header">🚀 Execute Generation</h3>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns([1, 1, 1], gap="large")
            
            with col2:
                if st.button("🔬 Generate Synthetic Data", key="synthetic_gen", use_container_width=True):
                    with st.spinner("🔄 Generating synthetic data..."):
                        try:
                            tables_with_invalid_data = []
                            for table in source_table_list:
                                if not has_valid_data(selected_source_database, selected_source_schema, table):
                                    tables_with_invalid_data.append(table)

                            if tables_with_invalid_data:
                                st.warning(f"⚠️ Insufficient data in: {', '.join(tables_with_invalid_data)}")
                                log_audit("Synthetic Data Generation failed due to insufficient data.", "FAILED", "synthetic")
                            else:
                                for table in selected_tables:
                                    join_keys = st.session_state.join_keys[table]
                                    
                                    if join_keys:
                                        for join_key in join_keys:
                                            sql_command = f"""
                                            CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA(
                                                {{
                                                    'datasets': [
                                                        {{
                                                            'input_table': '{selected_source_database}.{selected_source_schema}.{table}',
                                                            'output_table': '{selected_target_database}.{selected_target_schema}.{table}',
                                                            'columns': {{ '{join_key}':{{'join_key': True}} }}
                                                        }}
                                                    ],
                                                    'replace_output_tables': true
                                                }}
                                            );
                                            """
                                            session.sql(sql_command).collect()
                                    else:
                                        sql_command = f"""
                                        CALL SNOWFLAKE.DATA_PRIVACY.GENERATE_SYNTHETIC_DATA(
                                            {{
                                                'datasets': [
                                                    {{
                                                        'input_table': '{selected_source_database}.{selected_source_schema}.{table}',
                                                        'output_table': '{selected_target_database}.{selected_target_schema}.{table}'
                                                    }}
                                                ],
                                                'replace_output_tables': true
                                            }}
                                        );
                                        """
                                        session.sql(sql_command).collect()
                                
                                st.success("✅ Synthetic data generated successfully!", icon="🎉")
                                st.balloons()
                                log_audit("Synthetic Data Generation completed successfully.", "SUCCESS", "synthetic")

                        except Exception as e:
                            st.error(f"❌ Error executing synthetic data generation: {e}", icon="🚨")
                            log_audit("Synthetic Data Generation encountered an error.", "FAILED", "synthetic")

# Snowflake Masking with enhanced design
elif app_mode == "🔒 Snowflake Masking":
    session = get_active_session()

    # Navigation buttons for the masking
    app_mode_masking = st.sidebar.radio("Select Process", [
        "Home",
        "MASKING",
        "MASKING VALIDATION"  # New classification edit option
    ], index=0)

    # Home page for Snowflake Masking app
    if app_mode_masking == "Home":
        st.markdown('<h2 class="font">🛡️ Snowflake Masking App</h2>', unsafe_allow_html=True)
        st.markdown('<p>This application is designed to assist you with data masking in Snowflake and classfication edit and submission. Please follow each step to mask Snowflake schemas.</p>', unsafe_allow_html=True)

        # Overview of processes
        st.subheader('🔄 Overview of Processes:')
        st.markdown("""
        <div style="background: linear-gradient(135deg, #e8f4fd 0%, #c3d9ff 100%); padding: 2rem; border-radius: 15px; margin: 1rem 0;">
            <strong>ALTR Mapper:</strong><br>
            In this step, we are inserting classification results into the `ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS` table from the ALTR portal.
            <br><br>
            <strong>ALTR_CLASSIFICATION_DETAILS:</strong><br>
            In this step, we are transforming the classification results from `ALTR_DSAAS_DB.PUBLIC.CLASSIFICATION_DETAILS` into the `DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS` table.
            <br><br>
            <strong>TRANSFER_CLASSIFICATION_DETAILS:</strong><br>
            In this step, we transform the latest version of classification data from `DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS` into the `DEV_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS` table for the respective schema.
        </div>
        """, unsafe_allow_html=True)

    # Perform selections for Masking
    elif app_mode_masking == "MASKING":
        # Function to get databases based on prefix
        def get_databases(env):
            db_prefix = f"{env}_"
            db_query = f"""
            SELECT DATABASE_NAME
            FROM INFORMATION_SCHEMA.DATABASES
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
            AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
            """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        # Function to fetch schemas for a specific database
        def get_schemas(database_name):
            if not database_name:
                return []
            schema_query = f"SELECT SCHEMA_NAME FROM {database_name}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        # Function to fetch distinct BU names based on environment
        def get_bu_names(env):
            bu_query = f"SELECT DISTINCT BU_NAME FROM {env}_DB_MANAGER.MASKING.CONSUMER"
            try:
                rows = session.sql(bu_query).collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch BU names for environment {env}: {e}")
                return []

        # Input selections for masking environment
        masking_environment = st.selectbox("🌍 Masking Environment", ["DEV", "QA", "UAT", "PROD"])

        # Get databases based on the selected environment
        masking_database_list = get_databases(masking_environment)
        selected_masking_database = st.selectbox("📊 Database", masking_database_list)

        masking_schema_list = []
        selected_masking_schema = None
        if selected_masking_database:
            masking_schema_list = get_schemas(selected_masking_database)
            selected_masking_schema = st.selectbox("📁 Schema", masking_schema_list)

        # Determine selected_classification_database based on selected_masking_database
        selected_classification_database = None
        if selected_masking_database:
            # Split the database name by '_' and take the part after the environment prefix
            db_suffix = selected_masking_database.split('_', 1)[-1]
            selected_classification_database = f"PROD_{db_suffix}" # Always point to PROD

        # Keep selected_classification_schema the same as selected_masking_schema
        selected_classification_schema = selected_masking_schema

        # Get BU names based on the selected environment
        bu_name_list = get_bu_names(masking_environment)
        selected_bu_name = st.selectbox("🏢 BU Name", bu_name_list)

        # Get classification owner based on the new query criteria
        classification_owner_list = []
        if selected_classification_database and selected_classification_schema:
            owner_query = f"""
            WITH latest_import AS (
              SELECT MAX(import_id) AS max_id
              FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
              WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
            )
            SELECT DISTINCT classification_owner
            FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
            WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
              AND import_id = (SELECT max_id FROM latest_import);
            """
            try:
                rows = session.sql(owner_query).collect()
                classification_owner_list = [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch classification owner: {e}")
                classification_owner_list = []

        # Use classification owner from query results or fallback to "ALTR"
        selected_classification_owner = classification_owner_list[0] if classification_owner_list else "ALTR"

        # Button to execute all the masking processes
        if st.button("🚀 Run Masking"):
            if (selected_masking_database and selected_masking_schema and
                selected_bu_name and selected_classification_database and selected_classification_schema):

                # Track success of all operations
                success = True

                # Execute each process in sequence
                if selected_classification_owner == "ALTR":
                    try:
                        # Execute ALTR MAPPER
                        sql_command = f"""
                        CALL ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER(
                            MAPPING_FILE_PATH => BUILD_SCOPED_FILE_URL(@ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER_STAGE, 'gdlp-to-hipaa-map.json'),
                            TAG_DB => '{masking_environment}_DB_MANAGER',
                            TAG_SCHEMA => 'MASKING',
                            RUN_COMMENT => '{selected_classification_database} DATABASE CLASSIFICATION',
                            USE_DATABASES => '{selected_classification_database}',
                            EXECUTE_SQL => FALSE,
                            LOG_TABLE => 'CLASSIFICATION_DETAILS'
                        );
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ ALTR MAPPER executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing ALTR MAPPER: {str(e)}")
                        success = False

                    if success:
                        try:
                            # Execute ALTR CLASSIFICATION DETAILS
                            sql_command = f"CALL DEV_DB_MANAGER.MASKING.ALTR_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}')"
                            session.sql(sql_command).collect()
                            st.success("✅ ALTR CLASSIFICATION DETAILS executed successfully!")
                        except Exception as e:
                            st.error(f"❌ Error executing ALTR CLASSIFICATION DETAILS: {str(e)}")
                            success = False

                # Section for handling transfers when classification owner is NOT ALTR
                if selected_classification_owner != "ALTR":
                    try:
                        # Execute TRANSFER CLASSIFICATION DETAILS
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.TRANSFER_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}')"
                        session.sql(sql_command).collect()
                        st.success("✅ TRANSFER CLASSIFICATION DETAILS executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing TRANSFER CLASSIFICATION DETAILS: {str(e)}")
                        success = False

                # Metadata Refresh
                if success:
                    try:
                        # Execute Metadata Refresh
                        db_manager = f"{masking_environment}_DB_MANAGER"
                        # Pass the selected masking database to the procedure
                        sql_command = f"CALL {db_manager}.MASKING.UPDATE_METADATA_REFRESH_DATABASE('{selected_masking_database}')"
                        session.sql(sql_command).collect()
                        st.success("✅ Metadata Refresh executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing Metadata Refresh: {str(e)}")
                        success = False

                # Column Tag Mapping
                if success:
                    try:
                        # Execute COLUMN TAG MAPPING
                        sql_command = f"""
                        CALL {masking_environment}_DB_MANAGER.MASKING.COLUMN_TAG_MAPPING(
                            '{selected_classification_database}',
                            '{selected_classification_schema}',
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_classification_owner}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ COLUMN TAG MAPPING executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing COLUMN TAG MAPPING: {str(e)}")
                        success = False

                # Insert Data Output Final
                if success:
                    try:
                        # Execute INSERT DATA OUTPUT FINAL
                        sql_command = f"""
                        CALL {masking_environment}_DB_MANAGER.MASKING.INSERT_DATA_OUTPUT_FINAL(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_bu_name}',
                            '{selected_classification_owner}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ INSERT DATA OUTPUT FINAL executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing INSERT DATA OUTPUT FINAL: {str(e)}")
                        success = False

                 # Classification Generation
                if success:
                    try:
                        # Execute CLASSIFICATION_GENERATION
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}');"
                        session.sql(sql_command).collect()
                        st.success("✅ CLASSIFICATION_GENERATION executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CLASSIFICATION_GENERATION: {str(e)}")
                        success = False

                # Create Views
                if success:
                    try:
                        # Execute CREATE VIEWS
                        sql_command = f"""
                        CALL {masking_environment}_DB_MANAGER.MASKING.CREATE_VIEWS(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_masking_database}_MASKED',
                            '{selected_masking_schema}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ CREATE VIEWS executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CREATE VIEWS: {str(e)}")
                        success = False

                try:
                    if success:
                        audit_message = f"MASKING for {selected_masking_database}_MASKED.{selected_masking_schema}"
                        log_audit(audit_message, "Success", "masking")
                    else:
                        audit_message = f"MASKING for {selected_masking_database}_MASKED.{selected_masking_schema}"
                        log_audit(audit_message, "Failure", "masking")
                except Exception as e:
                    st.error(f"❌ Error logging audit: {str(e)}")

                if success:
                    st.success("✅ Completed all processes successfully!")
                else:
                    st.warning("Some steps failed. Please review the errors.")
            else:
                st.warning("Please ensure all selections are made before running the masking process.")
  
           
    elif app_mode_masking == "MASKING VALIDATION":
        # Define all functions inside this block

        def get_databases(env_prefix):
            db_prefix = f"{env_prefix}_"
            db_query = f"""
                SELECT DATABASE_NAME 
                FROM INFORMATION_SCHEMA.DATABASES 
                WHERE DATABASE_NAME LIKE '{db_prefix}%'
            """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        def get_schemas(database):
            schema_query = f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        def get_classification_owners(env):
            owner_query = f"""
                SELECT DISTINCT CLASSIFICATION_OWNER
                FROM {env}_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
            """
            rows = session.sql(owner_query).collect()
            return [row[0] for row in rows]

        def execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner):
            try:
                # Derive production database name
                production_database = selected_database.replace("DEV_", "PROD_").replace("QA_", "PROD_").replace("UAT_", "PROD_")
                # Query source tags
                source_tags_query = f"""
                SELECT COUNT(*) AS total_records
                FROM {env}_DB_MANAGER.MASKING.CLASSIFICATION_DETAILS
                WHERE "DATABASE" = '{production_database}'
                  AND "SCHEMA" = '{selected_schema}'
                  AND CLASSIFICATION_OWNER = '{classification_owner}'
                """
                # Query target tags
                target_tags_query = f"""
                SELECT COUNT(*) AS TAG_COUNT
                FROM {env}_DB_MANAGER.ACCOUNT_USAGE.TAG_REFERENCES
                WHERE OBJECT_DATABASE = '{selected_database}_MASKED'
                  AND OBJECT_SCHEMA = '{selected_schema}'
                """
                source_count = session.sql(source_tags_query).collect()[0][0]
                target_count = session.sql(target_tags_query).collect()[0][0]
                return source_count, target_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_tables(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_tables_query = f"""
                SELECT COUNT(TABLE_NAME) 
                FROM {selected_database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_CATALOG = '{selected_database}'
                  AND TABLE_SCHEMA = '{selected_schema}'
                  AND TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_NAME NOT LIKE 'RAW_%'
                  AND TABLE_NAME NOT LIKE 'VW_%'
                """
                validation_query = f"""
                SELECT COUNT(*) AS TABLE_COUNT
                FROM {db_manager}.MASKING.MD_TABLE t
                JOIN {db_manager}.MASKING.MD_SCHEMA s ON t.SCHEMA_ID = s.SCHEMA_ID
                JOIN {db_manager}.MASKING.MD_DATABASE d ON s.DATABASE_ID = d.DATABASE_ID
                WHERE d.DATABASE_NAME = '{selected_database}'
                  AND s.SCHEMA_NAME = '{selected_schema}'
                """
                table_count = session.sql(count_tables_query).collect()[0][0]
                validation_count = session.sql(validation_query).collect()[0][0]
                return table_count, validation_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_columns(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_columns_query = f"""
                SELECT COUNT(c.COLUMN_NAME) AS COLUMN_COUNT
                FROM {selected_database}.INFORMATION_SCHEMA.COLUMNS c
                JOIN {selected_database}.INFORMATION_SCHEMA.TABLES t
                  ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
                WHERE c.TABLE_SCHEMA = '{selected_schema}'
                  AND t.TABLE_TYPE = 'BASE TABLE'
                  AND c.TABLE_NAME NOT LIKE 'RAW_%'
                  AND c.TABLE_NAME NOT LIKE 'VW_%'
                """
                validation_query = f"""
                SELECT COUNT(col.COLUMN_ID) AS COLUMN_COUNT
                FROM {db_manager}.MASKING.MD_DATABASE db
                JOIN {db_manager}.MASKING.MD_SCHEMA sc ON db.DATABASE_ID = sc.DATABASE_ID
                JOIN {db_manager}.MASKING.MD_TABLE tb ON sc.SCHEMA_ID = tb.SCHEMA_ID
                JOIN {db_manager}.MASKING.MD_COLUMN col ON tb.TABLE_ID = col.TABLE_ID
                WHERE db.database_name='{selected_database}'
                  AND sc.schema_name='{selected_schema}'
                  AND db.IS_ACTIVE = TRUE
                  AND sc.IS_ACTIVE = TRUE
                  AND tb.IS_ACTIVE = TRUE
                  AND col.IS_ACTIVE = TRUE
                """
                column_count = session.sql(count_columns_query).collect()[0][0]
                validation_count = session.sql(validation_query).collect()[0][0]
                return column_count, validation_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_views(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_tables_query = f"""
                SELECT COUNT(TABLE_NAME) 
                FROM {selected_database}.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_CATALOG = '{selected_database}'
                  AND TABLE_SCHEMA = '{selected_schema}'
                  AND TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_NAME NOT LIKE 'RAW_%'
                  AND TABLE_NAME NOT LIKE 'VW_%'
                """
                count_target_query = f"""
                SELECT COUNT(TABLE_NAME) 
                FROM {selected_database}_MASKED.INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_SCHEMA = '{selected_schema}'
                """
                table_count = session.sql(count_tables_query).collect()[0][0]
                validation_count = session.sql(count_target_query).collect()[0][0]
                return table_count, validation_count
            except Exception as e:
                return None, str(e)

        def execute_validation_queries_data_set(env, selected_database, selected_schema):
            try:
                db_manager = f"{env}_DB_MANAGER"
                count_columns_query = f"""
                SELECT COUNT(col.COLUMN_ID) AS COLUMN_COUNT
                FROM {db_manager}.MASKING.MD_DATABASE db
                JOIN {db_manager}.MASKING.MD_SCHEMA sc ON db.DATABASE_ID = sc.DATABASE_ID
                JOIN {db_manager}.MASKING.MD_TABLE tb ON sc.SCHEMA_ID = tb.SCHEMA_ID
                JOIN {db_manager}.MASKING.MD_COLUMN col ON tb.TABLE_ID = col.TABLE_ID
                WHERE db.database_name='{selected_database}'
                  AND sc.schema_name='{selected_schema}'
                  AND db.IS_ACTIVE = TRUE
                  AND sc.IS_ACTIVE = TRUE
                  AND tb.IS_ACTIVE = TRUE
                  AND col.IS_ACTIVE = TRUE
                """
                validation_query = f"""
                SELECT COUNT(*) AS total_records
                FROM (
                    SELECT DISTINCT
                        ds.data_output_id,
                        d.database_name,
                        s.schema_name,
                        t.table_name,
                        c.column_name
                    FROM {db_manager}.MASKING.DATA_SET ds
                    INNER JOIN {db_manager}.MASKING.MD_DATABASE d ON ds.database_id = d.database_id
                    INNER JOIN {db_manager}.MASKING.MD_SCHEMA s ON ds.schema_id = s.schema_id
                    INNER JOIN {db_manager}.MASKING.MD_TABLE t ON ds.TABLE_ID = t.TABLE_ID
                    INNER JOIN {db_manager}.MASKING.MD_COLUMN c ON ds.COLUMN_ID = c.COLUMN_ID
                    WHERE d.database_name = '{selected_database}'
                      AND s.schema_name = '{selected_schema}'
                      AND ds.data_output_id = (
                          SELECT MAX(ds1.data_output_id) 
                          FROM {db_manager}.MASKING.DATA_SET ds1
                          INNER JOIN {db_manager}.MASKING.MD_DATABASE d1 ON ds1.database_id = d1.database_id
                          INNER JOIN {db_manager}.MASKING.MD_SCHEMA s1 ON ds1.schema_id = s1.schema_id
                          WHERE d1.database_name = '{selected_database}'
                            AND s1.schema_name = '{selected_schema}'
                      )
                ) AS subquery
                """
                column_count = session.sql(count_columns_query).collect()[0][0]
                data_count = session.sql(validation_query).collect()[0][0]
                return column_count, data_count
            except Exception as e:
                return None, str(e)

        # User input selections
        env = st.selectbox("🌍 Select Environment", ["DEV", "QA", "UAT", "PROD"])
        database_list = get_databases(env)
        selected_database = st.selectbox("📊 Select Database", database_list, key="db_select")
        schema_list = get_schemas(selected_database)
        selected_schema = st.selectbox("📁 Select Schema", schema_list, key="schema_select")
        classification_owners = get_classification_owners(env)
        classification_owner = st.selectbox("👤 Select Classification Owner", classification_owners)

        if st.button("🚀 Run All Validations"):
            results = {}

            # Run all validations
            table_count, table_validation_count = execute_validation_queries_tables(env, selected_database, selected_schema)
            results['MD Tables'] = {
                "Source Count": table_count,
                "Target Count": table_validation_count,
            }
            column_count, column_validation_count = execute_validation_queries_columns(env, selected_database, selected_schema)
            results['MD Columns'] = {
                "Source Count": column_count,
                "Target Count": column_validation_count,
            }
            dataset_count, dataset_data_count = execute_validation_queries_data_set(env, selected_database, selected_schema)
            results['Data Set'] = {
                "Source Count": dataset_count,
                "Target Count": dataset_data_count,
            }
            view_table_count, validation_count = execute_validation_queries_views(env, selected_database, selected_schema)
            results['Views'] = {
                "Source Count": view_table_count,
                "Target Count": validation_count,
            }
            tags_source_count, tags_target_count = execute_validation_queries_tags(env, selected_database, selected_schema, classification_owner)
            results['Tags'] = {
                "Source Count": tags_source_count,
                "Target Count": tags_target_count,
            }

            # Display results
            for validation_type, counts in results.items():
                st.markdown(f"### {validation_type} Validation Results")
                if None in counts.values():
                    st.error(f"Error during {validation_type} validation.")
                else:
                    st.success(f"Source Count: {counts['Source Count']}, Target Count: {counts['Target Count']}")

            # Log audit (assuming your log_audit function is defined)
            log_audit("Validation process completed.", "SUCCESS", "masking_validation")
                    
elif app_mode == "🔐 Snowflake Encryption":
    session = get_active_session()

    # Navigation buttons for the encryption process
    app_mode_encryption = st.sidebar.radio("Select Process", [
        "Home",
        "ENCRYPTION"
    ], index=0)

    # Home page for Snowflake Encryption app
    if app_mode_encryption == "Home":
        st.markdown('<h2 class="font">🔐 Snowflake Encryption App</h2>', unsafe_allow_html=True)
        st.markdown(
            '<p>This application is designed to assist you with data encryption in Snowflake. '
            'Please follow each step to encrypt Snowflake schemas.</p>', unsafe_allow_html=True
        )

       # Overview of processes
        st.subheader('🔄 Overview of Processes:')
        st.markdown("""
        <div style="background: linear-gradient(135deg, #fff9e6 0%, #ffe066 100%); padding: 2rem; border-radius: 15px; margin: 1rem 0;">
            <strong>ENCRYPTION:</strong><br>
            This application enables you to encrypt Snowflake schema tables. You need to select the database and schema you wish to encrypt. Additionally, you must choose the target environment where the encrypted tables will be deployed. 
            All encrypted databases will have a `_ENCRYPT` suffix, e.g., `DEV_DATALAKE_ENCRYPT`
        </div>
        """, unsafe_allow_html=True)

         # Limitations & Workarounds
        st.subheader('⚠️ Limitations & Workarounds:')
        st.markdown("""
        <div style="background: linear-gradient(135deg, #ffe6e6 0%, #ffb3b3 100%); padding: 1.5rem; border-radius: 10px; margin: 1rem 0;">
            <strong>JOINS USING ENCRYPTED COLUMNS:</strong><br>
            Tables can be joined using encrypted columns. Join columns must be encrypted using the same <strong>KEY</strong>, <strong>TWEAK</strong>, and <strong>ALPHABET</strong>.
            <br><br>
            <strong>SINGLE/MULTIPOINT SEARCHES:</strong><br>
            Search values should be encrypted using the same <strong>KEY</strong>, <strong>TWEAK</strong>, and <strong>ALPHABET</strong>.
        </div>
        """, unsafe_allow_html=True)

    # Encryption process
    elif app_mode_encryption == "ENCRYPTION":
        session = get_active_session()

        import re
    # Then select Source Database
        def get_databases(env):
            db_prefix = f"{env}_"
            db_query = f"""
            SELECT DATABASE_NAME
            FROM INFORMATION_SCHEMA.DATABASES
            WHERE DATABASE_NAME LIKE '{db_prefix}%'
            AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
            """
            rows = session.sql(db_query).collect()
            return [row[0] for row in rows]

        # Function to fetch schemas for a specific database
        def get_schemas(database_name):
            if not database_name:
                return []
            schema_query = f"SELECT SCHEMA_NAME FROM {database_name}.INFORMATION_SCHEMA.SCHEMATA"
            rows = session.sql(schema_query).collect()
            return [row[0] for row in rows]

        # Function to fetch distinct BU names based on environment
        def get_bu_names(env):
            bu_query = f"SELECT DISTINCT BU_NAME FROM {env}_DB_MANAGER.MASKING.CONSUMER"
            try:
                rows = session.sql(bu_query).collect()
                return [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch BU names for environment {env}: {e}")
                return []

        # Input selections for masking environment
        encryption_environment = st.selectbox("🌍 Encryption Environment", ["DEV", "QA", "UAT", "PROD"])

        # Get databases based on the selected environment
        masking_database_list = get_databases(encryption_environment)
        selected_masking_database = st.selectbox("📊 Database", masking_database_list)

        masking_schema_list = []
        selected_masking_schema = None
        if selected_masking_database:
            masking_schema_list = get_schemas(selected_masking_database)
            selected_masking_schema = st.selectbox("📁 Schema", masking_schema_list)

        # Determine selected_classification_database based on selected_masking_database
        selected_classification_database = None
        if selected_masking_database:
            # Split the database name by '_' and take the part after the environment prefix
            db_suffix = selected_masking_database.split('_', 1)[-1]
            selected_classification_database = f"PROD_{db_suffix}" # Always point to PROD

        # Keep selected_classification_schema the same as selected_masking_schema
        selected_classification_schema = selected_masking_schema

        # Get BU names based on the selected environment
        bu_name_list = get_bu_names(encryption_environment)
        selected_bu_name = st.selectbox("🏢 BU Name", bu_name_list)

        # Get classification owner based on the new query criteria
        classification_owner_list = []
        if selected_classification_database and selected_classification_schema:
            owner_query = f"""
            WITH latest_import AS (
              SELECT MAX(import_id) AS max_id
              FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
              WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
            )
            SELECT DISTINCT classification_owner
            FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
            WHERE database_name = '{selected_classification_database}'
              AND schema_name = '{selected_classification_schema}'
              AND import_id = (SELECT max_id FROM latest_import);
            """
            try:
                rows = session.sql(owner_query).collect()
                classification_owner_list = [row[0] for row in rows]
            except Exception as e:
                st.warning(f"Could not fetch classification owner: {e}")
                classification_owner_list = []

        # Use classification owner from query results or fallback to "ALTR"
        selected_classification_owner = classification_owner_list[0] if classification_owner_list else "ALTR"

        # Button to execute all the masking processes
        if st.button("🚀 Run Encryption"):
            if (selected_masking_database and selected_masking_schema and
                selected_bu_name and selected_classification_database and selected_classification_schema):

                # Track success of all operations
                success = True

                # Execute each process in sequence
                if selected_classification_owner == "ALTR":
                    try:
                        # Execute ALTR MAPPER
                        sql_command = f"""
                        CALL ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER(
                            MAPPING_FILE_PATH => BUILD_SCOPED_FILE_URL(@ALTR_DSAAS_DB.PUBLIC.ALTR_TAG_MAPPER_STAGE, 'gdlp-to-hipaa-map.json'),
                            TAG_DB => '{encryption_environment}_DB_MANAGER',
                            TAG_SCHEMA => 'MASKING',
                            RUN_COMMENT => '{selected_classification_database} DATABASE CLASSIFICATION',
                            USE_DATABASES => '{selected_classification_database}',
                            EXECUTE_SQL => FALSE,
                            LOG_TABLE => 'CLASSIFICATION_DETAILS'
                        );
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ ALTR MAPPER executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing ALTR MAPPER: {str(e)}")
                        success = False

                    if success:
                        try:
                            # Execute ALTR CLASSIFICATION DETAILS
                            sql_command = f"CALL DEV_DB_MANAGER.MASKING.ALTR_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}')"
                            session.sql(sql_command).collect()
                            st.success("✅ ALTR CLASSIFICATION DETAILS executed successfully!")
                        except Exception as e:
                            st.error(f"❌ Error executing ALTR CLASSIFICATION DETAILS: {str(e)}")
                            success = False

                # Section for handling transfers when classification owner is NOT ALTR
                if selected_classification_owner != "ALTR":
                    try:
                        # Execute TRANSFER CLASSIFICATION DETAILS
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.TRANSFER_CLASSIFICATION_DETAILS('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}')"
                        session.sql(sql_command).collect()
                        st.success("✅ TRANSFER CLASSIFICATION DETAILS executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing TRANSFER CLASSIFICATION DETAILS: {str(e)}")
                        success = False

                # Insert Data Output Final
                if success:
                    try:
                        # Execute INSERT DATA OUTPUT FINAL
                        sql_command = f"""
                        CALL {encryption_environment}_DB_MANAGER.MASKING.INSERT_DATA_OUTPUT_FINAL_ENCRYPTION(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_bu_name}',
                            '{selected_classification_owner}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ INSERT DATA OUTPUT FINAL executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing INSERT DATA OUTPUT FINAL: {str(e)}")
                        success = False

                # Classification Generation
                if success:
                    try:
                        # Execute CLASSIFICATION_GENERATION
                        sql_command = f"CALL DEV_DB_MANAGER.MASKING.CLASSIFICATION_REPORT_V1('{selected_classification_database}', '{selected_classification_schema}', '{selected_classification_owner}');"
                        session.sql(sql_command).collect()
                        st.success("✅ CLASSIFICATION_GENERATION executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CLASSIFICATION_GENERATION: {str(e)}")
                        success = False


                # Create Tables
                if success:
                    try:
                        # Execute CREATE TABLES
                        sql_command = f"""
                        CALL {encryption_environment}_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
                            '{selected_masking_database}',  -- Use selected masking database
                            '{selected_masking_schema}',    -- Use selected masking schema
                            '{selected_masking_database}_ENCRYPT',
                            '{selected_masking_schema}'
                        )
                        """
                        session.sql(sql_command).collect()
                        st.success("✅ CREATE TABLES executed successfully!")
                    except Exception as e:
                        st.error(f"❌ Error executing CREATE TABLES: {str(e)}")
                        success = False
                # Insert a single audit record for the entire process
                try:
                    if success:
                        audit_message = f"ENCRYPTION for {selected_masking_database}_ENCRYPT.{selected_masking_schema}"
                        log_audit(audit_message, "Success", "encryption")
                    else:
                        audit_message = f"ENCRYPTION for {selected_masking_database}_ENCRYPT.{selected_masking_schema}"
                        log_audit(audit_message, "Failure", "encryption")
                except Exception as e:
                    st.error(f"❌ Error logging audit: {str(e)}")

                if success:
                    st.success("✅ Completed all processes successfully!")
                else:
                    st.warning("Some steps failed. Please review the errors.")
            else:
                st.warning("Please ensure all selections are made before running the masking process.")

elif app_mode == "📊 Classifications":
    session = get_active_session()

    # Main UI
    app_mode_classification = st.sidebar.radio("Select Process", ["Home", "📝 Classification Edit and Submission"], index=0)

    if app_mode_classification == "Home":
        st.markdown('<h2 class="font">📊 Classifications</h2>', unsafe_allow_html=True)
        st.markdown("""
        <div style="background: linear-gradient(135deg, #e8f5e8 0%, #d4edda 100%); padding: 2rem; border-radius: 15px; margin: 1rem 0;">
            <p style="font-size: 18px; color: #155724;">This page is designed to assist you with classification editing and submission in Snowflake.</p>
        </div>
        """, unsafe_allow_html=True)
       
        st.subheader('🔄 Overview of Processes:')
        st.markdown("""
        <div style="background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%); padding: 1.5rem; border-radius: 10px; margin: 1rem 0;">
            <p><strong>📊 Get Classification Report:</strong> Select a specific database and schema, then click "Get Classification Report" to review the classifications.</p>
            <p><strong>✏️ Edit Classifications:</strong> Review classifications based on the BU_APPROVAL_STATUS field. Select options such as APPROVED, MASKED, or NO MASKING NEEDED.</p>
            <p><strong>💾 Auto-Save:</strong> Changes are automatically saved as you edit. No manual save required!</p>
            <p><strong>📤 Submit Report:</strong> After reviewing and editing, submit the final report to complete the process.</p>
        </div>
        """, unsafe_allow_html=True)

    elif app_mode_classification == "📝 Classification Edit and Submission":
        # Session state initialization
        for key in ["report_fetched", "edited_df", "submitted", "confirm_submission", "last_save_time"]:
            if key not in st.session_state:
                st.session_state[key] = False if key not in ["edited_df", "last_save_time"] else None

        # Helper functions
        def fetch_databases():
            session = get_active_session()
            rows = session.sql("""
                SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES 
                WHERE DATABASE_NAME LIKE 'PROD_%' AND DATABASE_NAME NOT LIKE '%_MASKED%' AND DATABASE_NAME NOT LIKE '%_ENCRYPT%'
            """).collect()
            return [row[0] for row in rows]

        def fetch_schemas(database):
            session = get_active_session()
            rows = session.sql(f"SELECT SCHEMA_NAME FROM {database}.INFORMATION_SCHEMA.SCHEMATA").collect()
            return [row[0] for row in rows]

        def fetch_classification_report(database, schema):
            session = get_active_session()
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

        def auto_save_classification_report(df, database, schema):
            """Auto-save function that runs in background"""
            session = get_active_session()
            try:
                values = []
                for _, row in df.iterrows():
                    values.append(f"""(
                        '{database}', '{schema}', '{row['CLASSIFICATION_OWNER']}', '{row['DATE']}',
                        '{row['TABLE_NAME']}', '{row['COLUMN_NAME']}', '{row['CLASSIFICATION']}',
                        '{row['HIPAA_CLASS']}', '{row['MASKED']}', '{row['BU_APPROVAL_STATUS']}',
                        '{row['BU_COMMENTS']}', '{row['BU_ASSIGNEE']}', '{row['INFOSEC_APPROVAL_STATUS']}',
                        '{row['INFOSEC_APPROVER']}', '{row['INFOSEC_COMMENTS']}', 
                        true, {int(row['VERSION']) if row['VERSION'] is not None else 1}, {int(row['ID'])}
                    )""")
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
                        DATE = source.DATE,
                        DATABASE_NAME = source.DATABASE_NAME,
                        SCHEMA_NAME = source.SCHEMA_NAME,
                        TABLE_NAME = source.TABLE_NAME,
                        COLUMN_NAME = source.COLUMN_NAME,
                        CLASSIFICATION = source.CLASSIFICATION,
                        HIPAA_CLASS = source.HIPAA_CLASS,
                        MASKED = source.MASKED,
                        BU_APPROVAL_STATUS = source.BU_APPROVAL_STATUS,
                        BU_COMMENTS = source.BU_COMMENTS,
                        BU_ASSIGNEE = source.BU_ASSIGNEE,
                        INFOSEC_APPROVAL_STATUS = source.INFOSEC_APPROVAL_STATUS,
                        INFOSEC_APPROVER = source.INFOSEC_APPROVER,
                        INFOSEC_COMMENTS = source.INFOSEC_COMMENTS,
                        IS_ACTIVE = source.IS_ACTIVE,
                        CLASSIFICATION_OWNER = source.CLASSIFICATION_OWNER,
                        VERSION = source.VERSION
                    WHEN NOT MATCHED THEN INSERT (
                        DATABASE_NAME, SCHEMA_NAME, CLASSIFICATION_OWNER, DATE,
                        TABLE_NAME, COLUMN_NAME, CLASSIFICATION, HIPAA_CLASS,
                        MASKED, BU_APPROVAL_STATUS, BU_COMMENTS, BU_ASSIGNEE,
                        INFOSEC_APPROVAL_STATUS, INFOSEC_APPROVER, INFOSEC_COMMENTS,
                        IS_ACTIVE, VERSION, ID
                    )
                    VALUES (
                        source.DATABASE_NAME, source.SCHEMA_NAME, source.CLASSIFICATION_OWNER, source.DATE,
                        source.TABLE_NAME, source.COLUMN_NAME, source.CLASSIFICATION, source.HIPAA_CLASS,
                        source.MASKED, source.BU_APPROVAL_STATUS, source.BU_COMMENTS, source.BU_ASSIGNEE,
                        source.INFOSEC_APPROVAL_STATUS, source.INFOSEC_APPROVER, source.INFOSEC_COMMENTS,
                        source.IS_ACTIVE, source.VERSION, source.ID
                    )
                """
                session.sql(merge_sql).collect()
                st.session_state.last_save_time = time.time()
                return True
            except Exception as e:
                st.error(f"❌ Auto-save error: {e}")
                return False

        def insert_raw_classification_details(database, schema, bu_name):
            session = get_active_session()

            # Define mapping for classification owner and HIPAA class
            classification_mapping = {
                "I&E Business Intelligence": ("IE_BU", "IE_PII"),
                "PRICE": ("PRICE_BU", "PRICE_PII"),
                "Marketing": ("MARKETING_BU", "MARKETING_PII"),
                "ZDI Provider Intelligence": ("PROVIDER_BU", "PROVIDER_PII"),
                "ZDI Member Intelligence": ("MEMBER_BU", "MEMBER_PII"),
                "Payments Optimization": ("PAYMENTS_BU", "PAYMENTS_PII"),
                "ZDI Data Science Engineer": ("DSE_BU", "DSE_PII"),
            }

            # Get classification owner and HIPAA class based on bu_name
            classification_owner, hipaa_class = classification_mapping.get(bu_name, (None, None))

            if classification_owner is None or hipaa_class is None:
                st.error("Invalid BU Name selected. Please select a valid BU.")
                return False

            # Determine the maximum version for the specific combination of database, schema, and classification owner
            max_version_row = session.sql(f"""
                SELECT MAX(VERSION) 
                FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS 
                WHERE DATABASE_NAME = '{database}' 
                    AND SCHEMA_NAME = '{schema}' 
                    AND CLASSIFICATION_OWNER = '{classification_owner}'
            """).first()

            max_version = max_version_row[0] if max_version_row[0] is not None else 0
            new_version = max_version + 1  # Increment the version for the insert

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
                st.warning("No classification details available for insertion.")
                return False

            insert_values = []
            duplicate_count = 0  # Counter for duplicate records

            for row in classification_details:
                # Check for existing records to prevent duplicates
                existing_record_check = session.sql(f"""
                    SELECT COUNT(*) 
                    FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
                    WHERE DATABASE_NAME = '{database}'
                        AND SCHEMA_NAME = '{schema}'
                        AND CLASSIFICATION_OWNER = '{classification_owner}'
                        AND TABLE_NAME = '{row['TABLE_NAME']}'
                        AND COLUMN_NAME = '{row['COLUMN_NAME']}'
                        AND HIPAA_CLASS = '{hipaa_class}'
                        AND BU_APPROVAL_STATUS = '{row['BU_APPROVAL_STATUS']}'
                        AND BU_COMMENTS = '{row['BU_COMMENTS']}'
                        AND BU_ASSIGNEE = '{row['BU_ASSIGNEE']}'
                        AND INFOSEC_APPROVAL_STATUS = '{row['INFOSEC_APPROVAL_STATUS']}'
                        AND INFOSEC_APPROVER = '{row['INFOSEC_APPROVER']}'
                        AND INFOSEC_COMMENTS = '{row['INFOSEC_COMMENTS']}'
                        AND IS_ACTIVE = TRUE
                """).first()[0]

                if existing_record_check > 0:
                    duplicate_count += 1  # Increment the duplicate counter
                    continue

                max_import_id_row = session.sql("SELECT MAX(IMPORT_ID) FROM DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS").first()
                max_import_id = max_import_id_row[0] if max_import_id_row[0] is not None else 0
                new_import_id = max_import_id + 1

                # Mark existing records as inactive
                session.sql(f"""
                    UPDATE DEV_DB_MANAGER.MASKING.RAW_CLASSIFICATION_DETAILS
                    SET IS_ACTIVE = false
                    WHERE DATABASE_NAME = '{database}'
                        AND SCHEMA_NAME = '{schema}'
                        AND CLASSIFICATION_OWNER = '{classification_owner}'
                """).collect()

                insert_values.append(f"""(
                    {new_import_id}, '{row['DATE']}', '{database}', '{schema}', 
                    '{row['TABLE_NAME']}', '{row['COLUMN_NAME']}', 'HIPAA', 
                    '{hipaa_class}', '{row['BU_APPROVAL_STATUS']}', '{row['BU_COMMENTS']}', 
                    '{row['BU_ASSIGNEE']}', '{row['INFOSEC_APPROVAL_STATUS']}', 
                    '{row['INFOSEC_APPROVER']}', '{row['INFOSEC_COMMENTS']}', 
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

                try:
                    session.sql(insert_sql).collect()
                    return True
                except Exception as e:
                    st.error(f"Error inserting into RAW_CLASSIFICATION_DETAILS: {e}")
                    return False
            else:
                # Show a consolidated duplicate message
                if duplicate_count > 0:
                    st.info(f"{duplicate_count} records already exist for the specified classification criteria. Skipping these entries.")
                else:
                    st.info("No new records to insert.")
                return False

        # Function to fetch distinct BU names
        def get_bu_names():
            session = get_active_session()
            rows = session.sql("SELECT DISTINCT BU_NAME FROM DEV_DB_MANAGER.MASKING.CONSUMER").collect()
            return [row[0] for row in rows]

        # UI for classification report editing with enhanced styling
        st.markdown('<h1 class="font">📊 Classification Report Editor</h1>', unsafe_allow_html=True)

        # Auto-save indicator
        if st.session_state.last_save_time:
            current_time = time.time()
            if current_time - st.session_state.last_save_time < 3:  # Show for 3 seconds
                st.markdown("""
                <div class="auto-save-indicator show">
                    💾 Auto-saved!
                </div>
                """, unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            database = st.selectbox("📊 Select Database", fetch_databases())
        with col2:
            if database:
                schema = st.selectbox("📁 Select Schema", fetch_schemas(database))

        if database and schema and st.button("📊 Get Classification Report", type="primary"):
            data = fetch_classification_report(database, schema)
            if data:
                df = pd.DataFrame([row.as_dict() for row in data])
                try:
                    current_user = get_active_session().sql("SELECT CURRENT_USER()").collect()[0][0]
                except:
                    current_user = get_active_session().get_current_user()
                df['BU_ASSIGNEE'] = current_user
                st.session_state.edited_df = df
                st.session_state.report_fetched = True
                st.success("✅ Classification report loaded successfully!")
            else:
                st.warning("⚠️ No data found for the selected database and schema.")

        # Enhanced Data Editor with Auto-save - Using full width
        if st.session_state.report_fetched and st.session_state.edited_df is not None:
            st.markdown("### 📝 Edit Classification Report")
            st.markdown("""
            <div style="background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); padding: 1rem; border-radius: 10px; margin: 1rem 0;">
                <p style="margin: 0; color: #0d47a1;"><strong>💡 Note:</strong> Changes are automatically saved as you edit. No manual save required!</p>
            </div>
            """, unsafe_allow_html=True)

            # Ensure the relevant columns are treated as categories with specific options
            st.session_state.edited_df['BU_APPROVAL_STATUS'] = st.session_state.edited_df['BU_APPROVAL_STATUS'].astype('category')
            st.session_state.edited_df['BU_APPROVAL_STATUS'] = st.session_state.edited_df['BU_APPROVAL_STATUS'].cat.set_categories(['MASK', 'APPROVED', 'NO MASKING NEEDED'])

            st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'] = st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'].astype('category')
            st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'] = st.session_state.edited_df['INFOSEC_APPROVAL_STATUS'].cat.set_categories(['MASK', 'APPROVED', 'NO MASKING NEEDED'])

            # Enhanced data editor with full width and no container restrictions
            edited_df = st.data_editor(
                st.session_state.edited_df, 
                num_rows="dynamic", 
                use_container_width=True,
                height=700,
                key="classification_editor"
            )

            # Auto-save when data changes
            if not edited_df.equals(st.session_state.edited_df):
                auto_save_classification_report(edited_df, database, schema)
                st.session_state.edited_df = edited_df

            st.markdown("### 📤 Submit Classifications")
            st.markdown("""
            <div style="background: linear-gradient(135deg, #f3e5f5 0%, #e1bee7 100%); padding: 1.5rem; border-radius: 10px; margin: 1rem 0;">
                <p style="margin: 0; color: #4a148c;"><strong>⚠️ Important:</strong> Review all classifications before final submission. This action will process the report for production use.</p>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2 = st.columns([2, 1])
            with col1:
                bu_name = st.selectbox("🏢 Select BU Name", get_bu_names())
            with col2:
                if bu_name and st.button("📤 Submit Classifications", type="primary"):
                    success = insert_raw_classification_details(database, schema, bu_name)
                    if success:
                        st.success("✅ Classification details submitted successfully!")
                        st.balloons()

# Encryption section (similar enhanced implementation)
elif app_mode == "🔐 Snowflake Encryption":
    session = get_active_session()
    
    st.markdown('<h1 class="section-header">🔐 Snowflake Encryption</h1>', unsafe_allow_html=True)
    
    # Enhanced encryption implementation would go here...
    st.markdown("""
    <div class="info-card">
        <h3>🔐 Advanced Encryption Platform</h3>
        <p>Format-preserving encryption for Snowflake data with comprehensive key management and secure processing capabilities.</p>
    </div>
    """, unsafe_allow_html=True)

# Enhanced footer
st.markdown("""
<div style="margin-top: 4rem; padding: 3rem; background: linear-gradient(135deg, #2c3e50, #34495e); border-radius: 20px; text-align: center; color: white;">
    <h2 style="margin: 0 0 1rem 0; font-weight: 300;">🛡️ ZDC Data Governance Platform</h2>
    <p style="font-size: 1.2rem; margin-bottom: 1rem;">Comprehensive data protection and compliance solution</p>
    <p style="opacity: 0.8; margin: 0; font-size: 1rem;">Powered by advanced AI and machine learning technologies</p>
</div>
""", unsafe_allow_html=True)