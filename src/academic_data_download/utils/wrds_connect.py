import wrds

def connect_wrds(username, password):
    print(f"Connecting to WRDS with username: {username}")
    db = wrds.Connection(wrds_username=username, wrds_password=password, use_keyring=False)
    print("Connected to WRDS!")
    return db
