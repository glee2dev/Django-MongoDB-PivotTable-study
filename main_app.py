import streamlit as st
import pandas as pd
import json
from credentials import USER_CREDENTIALS
from processing import load_json_files, process_selected_files_1, process_selected_files_2, process_selected_files_3 # process_selected_files_4

# --- price url 
price_file = r"product_price_tag.csv"

# --- Session State Management for Login ---
def check_login():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.username = None

    if not st.session_state.logged_in:
        st.title("Secure Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_button = st.button("Login")

        if login_button:
            if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
                st.session_state.logged_in = True
                st.session_state.username = username
                st.success(f"Welcome, {username}!")
            else:
                st.error("Invalid Username or Password. Please check with CLUE team member.")
        st.stop()  # Stop the app until login is successful

# --- Main App ---

## --- for local access comment the check_login() 
check_login()  # Secure Login

# --- App Header ---
st.title("CLUE: JSON to CSV")
st.write("JSON 파일을 필요에 맞는 형태로 추출(CSV) 하는 앱입니다.")
st.write("공유받은 JSON 파일을 로드하신 후 아래 세부 선택 사항을 활용하여 데이터 전처리 후 사용하세요.")

# Step 1: File Upload
uploaded_files = st.file_uploader("Upload JSON files", type="json", accept_multiple_files=True)
filenames = [file.name for file in uploaded_files]

if uploaded_files:
    # Load JSON files into DataFrames
    st.subheader("Uploaded Files")
    dataframes = load_json_files(uploaded_files)

    if dataframes:
        # Display Tabs for Uploaded Files
        st.subheader("Preview of the Selected Files")
        tab_names = list(dataframes.keys())
        tabs = st.tabs(tab_names)

        for i, tab in enumerate(tabs):
            with tab:
                st.write(f"**Preview of {tab_names[i]}**")
                st.dataframe(dataframes[tab_names[i]])  # Display DataFrame

        # Step 2: Select Files for Processing
        st.subheader("Select Files for Processing")
        selected_file = st.selectbox("Select a file to process", options=tab_names)
        # selected_files = st.multiselect("Select files", options=tab_names, default=tab_names)

        if selected_file:
            selected_dataframes = [dataframes[selected_file]] # [dataframes[file] for file in selected_files]
            filenames = [selected_file]
            # print(selected_file, filenames)

            # Step 3: Year Input and Processing Options
            tab1, tab2 = st.tabs(["구매기록-년도 수정", "Processing Options"])

            with tab1:
                st.subheader("지역 선택")
                locations = st.multiselect("Processing Option 1,2,3 세부 선택사항 입니다. 희망하는 지역을 선택하세요. (default: KOR)", options=['KOR','US'], default=['KOR'])
                st.info(f"Filtering data: {','.join(locations)} 지역으로 전처리합니다.")
                st.subheader("세부 선택 항목")
                year_input = st.number_input(
                    "Processing Option 2 세부 선택사항 입니다. 희망하는 구매기록 년도를 입력하세요. (default: 2010년)", min_value=1900, max_value=2100, value=2010)
                st.info(f"Filtering data: {year_input}년도 이후 구매 데이터만 전처리합니다.")
                
                
            with tab2:
                st.subheader("Processing Options")
                # Process Option 1
                if st.button("Process Option 1: ID별 DEMOGRAPHY"):
                    # Process demographic data
                    demo_df = process_selected_files_1(selected_dataframes, filenames, locations=locations)
                    st.subheader("Processed DataFrame ID별 DEMOGRAPHY")
                    st.dataframe(demo_df)
                    
                    # extract the short source name for csv filename 
                    source_name = selected_file.split('_')[0] if '_' in selected_file else selected_file
                    export_filename = f"{source_name}_processed_demographics.csv"

                    # Download the DataFrame as a CSV
                    csv = demo_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="Download CSV: Option 1",
                        data=csv,
                        file_name=export_filename,
                        mime="text/csv"
                    )

                # Process Option 2
                if st.button(f"Process Option 2: ID별 구매기록-{year_input}"):
                    result_df = process_selected_files_2(
                        selected_dataframes, filenames, year=year_input, locations=locations, price_file=price_file)
                    st.subheader(f"Processed DataFrame: ID별 구매기록-{year_input}")
                    st.dataframe(result_df)

                    # extract the short source name for csv filename 
                    source_name = selected_file.split(
                        '_')[0] if '_' in selected_file else selected_file
                    
                    # Convert DataFrame to CSV with utf-8-sig encoding
                    csv = result_df.to_csv(index=False, encoding='utf-8-sig')
                    
                    st.download_button(
                        "Download CSV: Option 2",
                        data=csv,
                        file_name=f"processed_data_2_purchase_listup_{source_name}_{year_input}.csv",
                        mime="text/csv"
                    )

                # Process Option 3
                if st.button(f"Process Option 3: ID별 라이프이벤트+구매기록 {locations}"):
                    result_df = process_selected_files_3(
                        selected_dataframes, filenames, locations=locations, price_file=price_file)
                    st.subheader(f"Processed DataFrame: ID별 라이프이벤트+구매기록 {locations}")
                    st.dataframe(result_df)
                    
                    source_name = selected_file.split(
                        '_')[0] if '_' in selected_file else selected_file
                    
                    csv = result_df.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button("Download CSV: Option 3", csv, f"processed_data_3_{source_name}_lifeevent_purch_listup.csv", "text/csv")

                # # Process Option 4
                # if st.button("Process Option 4"):
                #     result_df = process_selected_files_4(selected_dataframes)
                #     st.subheader("Processed DataFrame (Option 4)")
                #     st.dataframe(result_df)
                #     csv = result_df.to_csv(index=False, encoding='utf-8-sig')
                #     st.download_button("Download Processed CSV (Option 4)", csv, "processed_data_4.csv", "text/csv")
        else:
            st.warning("Please select at least one file for processing.")
    else:
        st.error("No valid JSON files were loaded.")
else:
    st.info("Upload JSON files to begin.")
