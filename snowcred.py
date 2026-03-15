import os
import snowflake.connector

user = os.environ.get("SNOWFLAKE_USER")
password = os.environ.get("SNOWFLAKE_PASSWORD")

# candidate account strings to try (add any others you want to test)
candidates = [
    "ec09293",
    "ec09293.anstxpm",
    "ec09293.anstxpm.us-east-1",
    "ec09293.anstxpm.snowflakecomputing.com",
]

print("User:", user)
print("Trying account candidates:", candidates)

for acct in candidates:
    print("\n--- Trying account:", acct, "---")
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=acct,
            # short timeout so it doesn't hang too long
            login_timeout=10
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        ver = cur.fetchone()[0]
        print("SUCCESS with account:", acct, "version:", ver)
        cur.close()
        conn.close()
        break
    except Exception as e:
        print("Error for", acct, "->", repr(e))