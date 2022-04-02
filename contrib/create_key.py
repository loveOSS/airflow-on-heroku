# Used to generate the fernet key for security
from cryptography import fernet

print(fernet.Fernet.generate_key().decode("utf-8"))
