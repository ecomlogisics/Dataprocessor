import streamlit as st
import pandas as pd
import io
from datetime import datetime
from data_processor import process_dispatch_data, create_excel_report

def main():
    st.title("Ecom Dispatch Report")
    st.write("Upload your CSV file (up to 500MB) and get a formatted Excel report.")

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            st.dataframe(df.head())

            if st.button("Generate Dispatch Report"):
                with st.spinner('Processing...'):
                    next_day_df, same_day_df, montreal_df = process_dispatch_data(df)

                    # Show preview tabs
                    st.subheader("Report Preview")
                    tab1, tab2, tab3 = st.tabs(["Next Day", "Same Day", "Montreal"])
                    with tab1:
                        st.dataframe(next_day_df)
                    with tab2:
                        st.dataframe(same_day_df)
                    with tab3:
                        st.dataframe(montreal_df)

                    # Show metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Next Day Deliveries", len(next_day_df))
                    with col2:
                        st.metric("Same Day Deliveries", len(same_day_df))
                    with col3:
                        st.metric("Montreal Deliveries", len(montreal_df))

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    st.download_button(
                        "Download Report",
                        data=create_excel_report(next_day_df, same_day_df, montreal_df).getvalue(),
                        file_name=f"dispatch_report_{timestamp}.xlsx",
                        mime="application/vnd.ms-excel"
                    )
                    st.success("Report generated successfully!")

        except Exception as e:
            st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()