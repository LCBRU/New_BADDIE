

import pyodbc
import os

def delete_files_with_failed_test_status(conn_str):
    query = """
    SELECT StudyID AS Folder, FILENAME, 'V:\\Baddie\\AIMI\\' + StudyID + '\\' + FILENAME AS fullpath
    FROM [dbo].[ForTestingReview]
    WHERE test_priority = 1
    """
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            fullpath = row[2]
            try:
                if os.path.exists(fullpath):
                    os.remove(fullpath)
                    print(f"Deleted: {fullpath}")
                else:
                    print(f"File not found: {fullpath}")
            except Exception as e:
                print(f"Error deleting {fullpath}: {e}")
    except Exception as e:
        print(f"Database query failed: {e}")

if __name__ == "__main__":

    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=UHLSQLBRICCS01\\BRICCS01;"
        "Database=i2b2_app03_AIMI;"
        "Trusted_Connection=yes;"
    )
    delete_files_with_failed_test_status(conn_str)