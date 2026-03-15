import os
import snowflake.connector

user = os.environ.get("SNOWFLAKE_USER")
password = os.environ.get("SNOWFLAKE_PASSWORD")
account = os.environ.get("SNOWFLAKE_ACCOUNT")

print("Trying connection with:")
print("  user =", user)
print("  account =", account)

try:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        # optional: you can add warehouse/database/schema/role here if needed
    )
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_VERSION()")
    ver = cur.fetchone()[0]
    print("Connected — Snowflake version:", ver)
    cur.close()
    conn.close()
except Exception as e:
    print("Connection error:")
    print(e)