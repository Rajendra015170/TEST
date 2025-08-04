# Enhanced Features for Streamlit Classifications App

import streamlit as st
import pandas as pd
import time
import io

# 1. Auto-save function
def auto_save_classifications(df, database, schema, save_function):
    """Auto-save function with visual indicator"""
    try:
        if save_function(df, database, schema):
            # Show auto-save indicator
            placeholder = st.empty()
            placeholder.success(f"✅ Auto-saved at {time.strftime('%H:%M:%S')}")
            time.sleep(1)
            placeholder.empty()
            return True
    except Exception as e:
        st.error(f"Auto-save failed: {e}")
        return False

# 2. Enhanced statistics display
def show_enhanced_statistics(df):
    """Display enhanced statistics with percentages"""
    if df is not None and not df.empty:
        total = len(df)
        approved = len(df[df['BU_APPROVAL_STATUS'] == 'APPROVED'])
        mask = len(df[df['BU_APPROVAL_STATUS'] == 'MASK'])
        no_masking = len(df[df['BU_APPROVAL_STATUS'] == 'NO MASKING NEEDED'])
        pending = total - (approved + mask + no_masking)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Records", total)
        
        with col2:
            pct = (approved/total*100) if total > 0 else 0
            st.metric("Approved", f"{approved} ({pct:.1f}%)")
        
        with col3:
            pct = (mask/total*100) if total > 0 else 0
            st.metric("Mask Required", f"{mask} ({pct:.1f}%)")
        
        with col4:
            pct = (no_masking/total*100) if total > 0 else 0
            st.metric("No Masking", f"{no_masking} ({pct:.1f}%)")
        
        with col5:
            pct = (pending/total*100) if total > 0 else 0
            st.metric("Pending", f"{pending} ({pct:.1f}%)")
        
        return {'total': total, 'approved': approved, 'mask': mask, 'no_masking': no_masking, 'pending': pending}

# 3. CSV download function
def get_csv_download_link(df, filename):
    """Generate CSV download link"""
    csv = df.to_csv(index=False)
    return csv.encode('utf-8')

# 4. Enhanced filtering
def apply_filters(df):
    """Apply enhanced filtering to dataframe"""
    st.subheader("🔍 Filters and Search")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        tables = ['All'] + sorted(df['TABLE_NAME'].unique().tolist())
        selected_table = st.selectbox("Filter by Table", tables)
    
    with col2:
        statuses = ['All'] + sorted(df['BU_APPROVAL_STATUS'].unique().tolist())
        selected_status = st.selectbox("Filter by Status", statuses)
    
    with col3:
        hipaa_classes = ['All'] + sorted(df['HIPAA_CLASS'].unique().tolist())
        selected_hipaa = st.selectbox("Filter by HIPAA Class", hipaa_classes)
    
    with col4:
        search_term = st.text_input("Search Columns", placeholder="Enter column name...")
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_table != 'All':
        filtered_df = filtered_df[filtered_df['TABLE_NAME'] == selected_table]
    
    if selected_status != 'All':
        filtered_df = filtered_df[filtered_df['BU_APPROVAL_STATUS'] == selected_status]
    
    if selected_hipaa != 'All':
        filtered_df = filtered_df[filtered_df['HIPAA_CLASS'] == selected_hipaa]
    
    if search_term:
        filtered_df = filtered_df[filtered_df['COLUMN_NAME'].str.contains(search_term, case=False, na=False)]
    
    st.info(f"Showing {len(filtered_df)} of {len(df)} records")
    
    return filtered_df

# 5. Enhanced CSS
enhanced_css = """
<style>
/* Modern styling */
.stApp {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.main .block-container {
    background: rgba(255, 255, 255, 0.95);
    border-radius: 15px;
    padding: 2rem;
    margin: 1rem;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
}

.stButton button {
    background: linear-gradient(45deg, #3498db, #2980b9);
    color: white;
    border-radius: 25px;
    border: none;
    padding: 12px 24px;
    font-weight: 600;
    box-shadow: 0 4px 15px rgba(52, 152, 219, 0.3);
    transition: all 0.3s ease;
}

.stButton button:hover {
    background: linear-gradient(45deg, #2980b9, #1a6695);
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(52, 152, 219, 0.4);
}

.stSelectbox > div > div {
    background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
    color: white;
    border-radius: 10px;
    border: 2px solid #74b9ff;
}

.stTextInput > div > div > input {
    background: linear-gradient(135deg, #fd79a8 0%, #e84393 100%);
    color: white;
    border-radius: 10px;
    border: 2px solid #fd79a8;
}

.stMetric {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1rem;
    border-radius: 10px;
    color: white;
    text-align: center;
}
</style>
"""

