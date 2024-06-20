# Import required Libraries
import streamlit as st
from snowflake.snowpark.session import Session
import pandas as pd

def main():
    st.title("Snowflake Table Uploader & Downloader")

    #db_session_info = get_db_session_info()
    #if db_session_info is None: return

    #session = connecting_db(db_session_info)
    session = connecting_db()
    if not session: return

    db_name,schema_name,table_name = get_obj_name(session)
    if table_name is None: return

    col1, col2 = st.columns(2)
    with col1:
        st.header("Upload")
        uploaded_file = st.file_uploader("Choose a file")
    with col2:
        st.header("Download")
        download_table_name = f"{db_name}.{schema_name}.{table_name}"

        #local
        #if st.button("Dounload"):
        #    download(session,download_table_name)

        data_list = session.sql(f"select * from {download_table_name}").collect()
        df = pd.DataFrame(data_list).to_csv(index=False).encode('utf-8')
        if st.download_button(
            label="Download",
            data=df,
            file_name=f"{db_name}_{schema_name}_{table_name}.csv",
            mime="text/csv"):
            st.write(f'Your download of file "{download_table_name}" was a success. You downloaded {len(data_list)} rows.')

    if uploaded_file is not None:
        upload_df = pd.read_csv(uploaded_file)
        session.write_pandas(
            upload_df,
            table_name=table_name,
            database=db_name, 
            schema=schema_name, 
            chunk_size=None,
            overwrite=True,
            quote_identifiers=False
        )
        st.write(f'Your upload of file "{uploaded_file.name}" was a success. You uploaded {len(upload_df)} rows.')
        #st.experimental_rerun()
        #uploaded_file = None

    #session.close()

@st.cache_resource(experimental_allow_widgets=True) 
def get_db_session_info():
    db_session_info = st.file_uploader(
        '''
        Upload your Snowflake connection information csv file.
        csv example)
        account,user,password,role,warehouse
        XX99999.ap-northeast-1.aws,user_a,password_a,role_a,warehouse_a
        '''
    )
    return db_session_info

#@st.cache_resource
@st.cache_resource(experimental_allow_widgets=True) 
#def connecting_db(db_session_info):
def connecting_db():
    db_session_info = st.file_uploader(
        '''
        Upload your Snowflake connection information csv file.
        csv example)
        account,user,password,role,warehouse
        XX99999.ap-northeast-1.aws,user_a,password_a,role_a,warehouse_a
        '''
    )
    if db_session_info is None: return None
    df = pd.read_csv(db_session_info)
    connection_parameters = {
        "account": df.account[0],
        "user": df.user[0],
        "password": df.password[0],
        "role": df.role[0],
        "warehouse": df.warehouse[0],
        #"database": database,
        #"schema": schema 
        }
    session = Session.builder.configs(connection_parameters).create()
    return session

def get_obj_name(session):
    db_name=schema_name=table_name=None

    db_name_list = get_name_list(session, "SHOW DATABASES")
    if db_name_list.empty: return db_name,schema_name,table_name
    db_name = st.selectbox('Select Database', db_name_list)

    schema_name_list = get_name_list(session, f"SHOW SCHEMAS IN DATABASE {db_name}")
    if schema_name_list.empty: return db_name,schema_name,table_name
    schema_name = st.selectbox('Select Schema', schema_name_list)

    table_name_list = get_name_list(session, f"SHOW TABLES IN SCHEMA {db_name}.{schema_name}")
    if table_name_list.empty: return db_name,schema_name,table_name
    table_name = st.selectbox('Select Table', table_name_list)

    return db_name,schema_name,table_name

@st.cache_data()
def get_name_list(_session,command):
    o_list = _session.sql(command).collect()
    name_list = pd.DataFrame(o_list)
    if not name_list.empty:
        name_list = name_list["name"]
    return name_list

@st.cache_data()
def download(session,download_table_name):
    data_list = session.sql(f"select * from {download_table_name}").collect()
    df = pd.DataFrame(data_list)
    filename = download_table_name.replace(".","_")
    df = df.to_csv(f"{filename}.csv",index=False)
    st.write(f'Your download of file "{filename}" was a success. You downloaded {len(data_list)} rows.')

if __name__ == "__main__":
    main()