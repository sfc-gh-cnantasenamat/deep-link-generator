import streamlit as st
import re
import random
import qrcode
from io import BytesIO
import pandas as pd
# Import Snowpark - this will be used to interact with Snowflake
from snowflake.snowpark.context import get_active_session

# Note: You may need to install the qrcode and Pillow libraries:
# pip install qrcode pillow
    
# Try to get the active Snowflake session.
# If running locally or outside of a Snowflake environment, session will be None.
try:
    session = get_active_session()
except Exception as e:
    session = None

# --- Data Loading and Caching Functions ---
@st.cache_data(ttl=3600) # Cache the user's name for 1 hour
def get_user_full_name(_session):
    """
    Fetches the full name of the current Snowflake user by querying
    the ACCOUNT_USAGE.USERS view.
    """
    if not _session: return "Unknown User"
    try:
        user_login_name = _session.sql("SELECT CURRENT_USER();").collect()[0][0]
        user_details_df = _session.sql(
            "SELECT FIRST_NAME, LAST_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE name = ? AND deleted_on IS NULL",
            params=[user_login_name]
        ).to_pandas()
        if not user_details_df.empty:
            first_name = user_details_df['FIRST_NAME'].iloc[0]
            last_name = user_details_df['LAST_NAME'].iloc[0]
            full_name = ' '.join(filter(None, [first_name, last_name]))
            return full_name if full_name.strip() else user_login_name
        return user_login_name
    except Exception:
        return "Unknown User"

@st.cache_data(ttl=60) # Cache log data for 1 minute
def load_log_data(_session):
    """Loads all data from the log table for editing."""
    if not _session: return pd.DataFrame()
    try:
        return _session.table("DEEPLINK_DB.PUBLIC.DEEPLINK_DATA_LOG").to_pandas()
    except Exception:
        return pd.DataFrame()

# --- Helper Functions ---
def infer_product_from_url(url):
    """Tries to infer a product name from the Snowflake URL path."""
    if not url or not url.strip(): return ""
    match_fragment = re.search(r"#/(?:([^/]+))", url)
    if match_fragment: return match_fragment.group(1).replace('-', ' ').title()
    match_path = re.search(r"app\.snowflake\.com/([^/?#]+)", url)
    if match_path: return match_path.group(1).replace('-', ' ').title()
    return "Unknown"

def generate_deeplink(url_input):
    """Converts a standard Snowflake URL to its deeplink equivalent."""
    if url_input == "Input Here": return "Your deeplink to use will appear here"
    if not url_input or not url_input.strip(): return ""
    if re.search(r"#/|/console/", url_input):
        return re.sub(r"^(https://app\.snowflake\.com/)[^#]+([#/|/console/].*)$", r"\1_deeplink\2", url_input)
    else:
        base_deeplink = "https://app.snowflake.com/_deeplink/"
        match = re.search(r"^https://app\.snowflake\.com/(.*)$", url_input)
        path = match.group(1) if match else ""
        path = re.sub(r"^[^/]+/[^/]+/?", "", path)
        path = re.sub(r"//+", "/", path)
        path = re.sub(r"^/", "", path)
        return base_deeplink + path

def generate_utm_url(content_title, base_url, source):
    """Generates a URL with UTM parameters."""
    if content_title == "Title": return ""
    utm_params_map = {
        "Quickstart": "utm_source=quickstart&utm_medium=quickstart",
        "LinkedIn": "utm_source=linkedin&utm_medium=social",
        "Medium": "utm_source=medium_blog&utm_medium=blog",
        "GitHub": "utm_source=github&utm_medium=github",
        "Docs": "utm_source=docs&utm_medium=docs"
    }
    utm_params = utm_params_map.get(source)
    if not utm_params: return ""
    separator = "?" if source == "Quickstart" else "/?"
    content_prefix = "app-" if source == "Quickstart" else "-app-"
    content_slug = content_title.lower().replace(" ", "-")
    return f"{base_url}{separator}{utm_params}&utm_campaign=-us-en-all&utm_content={content_prefix}{content_slug}"

# Load user's name
all_log_data = load_log_data(session)
current_user_name = get_user_full_name(session)

# Home page
logo_image_url = "https://github.com/sfc-gh-cnantasenamat/sf-img/blob/main/img/snowflake-logo.png?raw=true"
icon_image_url = "https://github.com/sfc-gh-cnantasenamat/sf-img/blob/main/img/snowflake-icon.png?raw=true"
st.logo(logo_image_url, icon_image=icon_image_url)

with st.sidebar:
    st.title("Deep Link Generator")
    st.write(":material/person: Hi,", current_user_name, "!")
    st.info("Use this deep link generator to generate trackable links for Snowsight.")

st.warning("Generate your own trackable Snowsight links here")

# --- Main App Container ---
with st.container(border=True):
    # **NEW**: Mode selector for adding or editing
    mode = st.radio(":material/add_link: Mode", ["Add New Link", "Edit Existing Link"], horizontal=True)
    
    # Initialize session_state variables
    if 'submitted' not in st.session_state: st.session_state.submitted = False
    if 'qr_code_bytes' not in st.session_state: st.session_state.qr_code_bytes = None
    
    # --- EDIT MODE ---
    if mode == "Edit Existing Link":
        if not session:
            st.error("A Snowflake session is required to edit existing data.")
        else:
            # all_log_data = load_log_data(session) # Already loaded outside the container
            # current_user_name = get_user_full_name(session) # Already loaded
            
            # **MODIFIED**: Filter data to only show records created by the current user.
            user_log_data = all_log_data[all_log_data['NAME'] == current_user_name]

            if user_log_data.empty:
                st.warning("You have not created any links to edit.")
            else:
                # **MODIFIED**: Create a user-friendly display for the selectbox using the INPUT_URL.
                user_log_data['DISPLAY'] = user_log_data['LOG_ID'].astype(str) + " - " + user_log_data['INPUT_URL'].fillna('No URL')
                
                selected_display = st.selectbox("Select a record to edit", user_log_data['DISPLAY'])
                
                # Find the selected record's data
                selected_log_id = int(selected_display.split(" - ")[0])
                record_to_edit = user_log_data[user_log_data['LOG_ID'] == selected_log_id].iloc[0]

                # Populate form with existing data
                st.text_input("Paste your Snowflake link here", value=record_to_edit['INPUT_URL'], key="input_url_value")
                st.text_input("Your Name", value=record_to_edit['NAME'], key="name_input_value")
                st.text_input("Product Name", value=record_to_edit['PRODUCT'], key="product_input_value")
                st.text_input("Content Title", value=record_to_edit['CONTENT_TITLE'], key="content_title_value")
                
                source_options = ["Quickstart", "LinkedIn", "Medium", "GitHub", "Docs"]
                status_options = ["In Progress", "Link Works!", "Blocked", "Live to Customers"]
                
                st.selectbox("Promotion Source", source_options, index=source_options.index(record_to_edit['SOURCE']) if record_to_edit['SOURCE'] in source_options else 0, key="source_selection")
                st.selectbox("Status", status_options, index=status_options.index(record_to_edit['STATUS']) if record_to_edit['STATUS'] in status_options else 0, key="status_selection")

                # --- NEW: Display the creation and last updated timestamps ---
                st.caption(f"Created on: {record_to_edit['CREATION_DATE']}")
                st.caption(f"Last Updated: {record_to_edit['LAST_UPDATED_DATE']}")
                # -----------------------------------------------------------------

                message_placeholder = st.empty()
                
                if st.button("Update", type="primary", use_container_width=True):
                    # Update logic
                    try:
                        # --- MODIFIED: Update the LAST_UPDATED_DATE field ---
                        session.sql(
                            """
                            UPDATE DEEPLINK_DB.PUBLIC.DEEPLINK_DATA_LOG
                            SET
                                NAME = ?,
                                PRODUCT = ?,
                                STATUS = ?,
                                CONTENT_TITLE = ?,
                                SOURCE = ?,
                                INPUT_URL = ?,
                                LAST_UPDATED_DATE = CURRENT_TIMESTAMP()
                            WHERE LOG_ID = ?
                            """,
                            params=[
                                st.session_state.name_input_value,
                                st.session_state.product_input_value,
                                st.session_state.status_selection,
                                st.session_state.content_title_value,
                                st.session_state.source_selection,
                                st.session_state.input_url_value,
                                selected_log_id
                            ]
                        ).collect()
                        # ----------------------------------------------------
                        message_placeholder.success(f"✅ Record {selected_log_id} updated successfully!")
                        st.cache_data.clear() # Clear cache to reflect changes
                    except Exception as e:
                        message_placeholder.error(f"An error occurred while updating the database: {e}")

    # --- ADD NEW LINK MODE ---
    else:
        # Initialize name in session state for 'Add' mode
        if 'name_input_value' not in st.session_state or st.session_state.get('mode') != 'Add':
            st.session_state.name_input_value = get_user_full_name(session) if session else ""
        st.session_state.mode = 'Add'

        example_urls = [
            "https://app.snowflake.com/kl30547/us-east-2/#/cortex/playground",
            "https://app.snowflake.com/kl30547/us-east-2/#/agents",
            "https://app.snowflake.com/marketplace",
            "https://app.snowflake.com/migrations",
            "https://app.snowflake.com/openflow"
        ]

        def use_example_url():
            st.session_state.input_url_value = random.choice(example_urls)
            st.session_state.submitted = False

        st.text_input("Paste your Snowsight link here", key="input_url_value")
        st.text_input("Your Name", key="name_input_value")
        st.text_input("Product Name", value=infer_product_from_url(st.session_state.input_url_value), key="product_input_value")
        st.text_input("Content Title", "My Awesome Post", key="content_title_value")
        
        utm_params_map = {"Quickstart": "...", "LinkedIn": "...", "Medium": "...", "GitHub": "...", "Docs": "..."}
        st.selectbox("Promotion Source", list(utm_params_map.keys()), key="source_selection")
        st.selectbox("Status", ["In Progress", "Link Works!", "Blocked", "Live to Customers"], key="status_selection")

        message_placeholder = st.empty()
        col1, col2 = st.columns(2)

        with col1:
            if st.button("Submit", type="primary", use_container_width=True):
                st.session_state.submitted = True
                generated_deeplink = generate_deeplink(st.session_state.input_url_value)
                final_tracking_url = generate_utm_url(st.session_state.content_title_value, generated_deeplink, st.session_state.source_selection)
                st.session_state.final_tracking_url = final_tracking_url

                if session:
                    try:
                        check_df = session.sql(
                            "SELECT COUNT(*) FROM DEEPLINK_DB.PUBLIC.DEEPLINK_DATA_LOG WHERE INPUT_URL = ? AND CONTENT_TITLE = ? AND SOURCE = ?",
                            params=[st.session_state.input_url_value, st.session_state.content_title_value, st.session_state.source_selection]
                        ).collect()
                        
                        if check_df[0][0] > 0:
                            message_placeholder.warning("⚠️ This combination of URL, Title, and Source already exists.")
                        else:
                            session.sql(
                                """
                                INSERT INTO DEEPLINK_DB.PUBLIC.DEEPLINK_DATA_LOG 
                                (NAME, PRODUCT, STATUS, CONTENT_TITLE, SOURCE, INPUT_URL, GENERATED_DEEPLINK, TRACKING_URL) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                params=[
                                    st.session_state.name_input_value, st.session_state.product_input_value, st.session_state.status_selection,
                                    st.session_state.content_title_value, st.session_state.source_selection, st.session_state.input_url_value,
                                    generated_deeplink, final_tracking_url
                                ]
                            ).collect()
                            message_placeholder.success("✅ Link generation data saved successfully!")
                            st.cache_data.clear()
                    except Exception as e:
                        message_placeholder.error(f"An error occurred while writing to the database: {e}")

                if final_tracking_url:
                    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
                    qr.add_data(final_tracking_url)
                    qr.make(fit=True)
                    img = qr.make_image(fill_color="black", back_color="white")
                    buf = BytesIO()
                    img.save(buf, format="PNG")
                    st.session_state.qr_code_bytes = buf.getvalue()
                else:
                    st.session_state.qr_code_bytes = None

        with col2:
            st.button("Use Example", on_click=use_example_url, use_container_width=True)

# Display URLs and QR code after submission in 'Add' mode
if st.session_state.submitted and mode == 'Add New Link':
    with st.container(border=True):
        st.subheader(":material/link: Generated Links & QR Code")
        generated_deeplink = generate_deeplink(st.session_state.input_url_value)
        st.caption(f"Input URL: {st.session_state.input_url_value}")
        st.caption(f"Generated Deep Link: {generated_deeplink}")
        st.caption("Generated Deep Link with Tracking:")
        st.code(st.session_state.get("final_tracking_url", ""))
        
        if st.session_state.qr_code_bytes:
            qr_col = st.columns([1,5,1])
            with qr_col[1]:
                st.image(st.session_state.qr_code_bytes, caption='Scan for Deep Link with Tracking')
                st.download_button(
                    label="Download QR Code", data=st.session_state.qr_code_bytes,
                    file_name="qr_code.png", mime="image/png", type="primary", use_container_width=True
                )
